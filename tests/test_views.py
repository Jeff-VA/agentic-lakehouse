import re
from datetime import UTC, datetime, timedelta

import duckdb
import pytest
import sqlglot
import yaml

from core.compiler import Filter, MetricQuery, OrderBy
from core.views import View, ViewError, load_views, parse_view
from test_compiler import ONTOLOGY

NOW = datetime(2026, 1, 1, 6, 30, tzinfo=UTC)
TABLES = {
    "raw.playback": {"ts": "TIMESTAMP", "isp": "VARCHAR", "device": "VARCHAR", "user_id": "VARCHAR", "latency_ms": "INTEGER"},
    "gold.hourly": {"ts": "TIMESTAMP", "isp": "VARCHAR", "plays": "BIGINT", "slow_rate": "DOUBLE"},
}
LAST_HOUR = "ts >= now() - INTERVAL '1' HOUR"


def view(sql: str, params: dict | None = None, description: str = "A test view.") -> View:
    header = yaml.safe_dump({"description": description} | ({"params": params} if params else {}))
    return parse_view("test", f"/*\n{header}*/\n{sql}", ONTOLOGY, TABLES)


def error(sql: str, params: dict | None = None) -> str:
    with pytest.raises(ViewError) as raised:
        view(sql, params)
    return str(raised.value)


# ---------------------------------------------------------------- views over models


def test_a_model_view_becomes_a_metric_query():
    v = view(
        """
        SELECT date_trunc('hour', ts) AS hour, isp, plays, slow_rate AS slow
        FROM playback
        WHERE ts >= now() - INTERVAL :hours HOUR AND isp IN ('comcast', 'att') AND is_mobile = :mobile
        ORDER BY slow DESC, hour
        LIMIT 10
        """,
        {"hours": {"type": "int", "default": 6}, "mobile": {"type": "boolean", "default": None}},
    )
    assert v.reads == ["playback"]
    assert [(c.name, c.type, c.role) for c in v.columns] == [
        ("hour", "timestamp", "time"),
        ("isp", "string", "dimension"),
        ("plays", "number", "metric"),
        ("slow", "number", "metric"),
    ]
    expected = MetricQuery(
        metrics=["plays", "slow_rate"],
        dimensions=["isp"],
        grain="hour",
        start=NOW - timedelta(hours=6),
        end=NOW,
        filters=[Filter(dimension="isp", op="in", value=["comcast", "att"])],
        order_by=[OrderBy(field="slow_rate", descending=True), OrderBy(field="ts")],
        limit=10,
    )
    assert v.source.query(v.bind({}), NOW) == expected
    with_mobile = v.source.query(v.bind({"mobile": "true"}), NOW)
    assert with_mobile.filters[-1] == Filter(dimension="is_mobile", op="=", value=True)


HOURS = {"n": {"type": "int", "default": 1}}
SPAN = {"since": {"type": "timestamp"}, "until": {"type": "timestamp"}}


@pytest.mark.parametrize(
    ("where", "params", "values", "start", "end"),
    [
        ("ts >= now() - INTERVAL '90' MINUTE", None, {}, NOW - timedelta(minutes=90), NOW),
        ("ts >= now() - :n * INTERVAL '1' HOUR", HOURS, {"n": 3}, NOW - timedelta(hours=3), NOW),
        ("ts >= now() - INTERVAL :n DAY", HOURS, {"n": 2}, NOW - timedelta(days=2), NOW),
        ("now() - INTERVAL '2' HOUR <= ts", None, {}, NOW - timedelta(hours=2), NOW),  # reversed comparison
        (
            "ts >= date_trunc('day', now()) - INTERVAL '7' DAY AND ts < date_trunc('day', now())",
            None,
            {},
            datetime(2025, 12, 25, tzinfo=UTC),
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
        (
            "ts >= TIMESTAMP '2025-12-31 00:00:00' AND ts < TIMESTAMP '2026-01-01 03:00:00'",
            None,
            {},
            datetime(2025, 12, 31, tzinfo=UTC),
            datetime(2026, 1, 1, 3, tzinfo=UTC),
        ),
        (
            "ts >= :since AND ts < :until",
            SPAN,
            {"since": "2026-01-01T01:00:00Z", "until": "2026-01-01T03:00:00+01:00"},
            datetime(2026, 1, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 1, 2, tzinfo=UTC),
        ),
    ],
)
def test_time_ranges(where, params, values, start, end):
    v = view(f"SELECT plays FROM playback WHERE {where}", params)
    query = v.source.query(v.bind(values), NOW)
    assert (query.start, query.end) == (start, end)


def test_an_optional_parameter_without_a_value_drops_its_condition():
    v = view(
        f"SELECT isp, plays FROM playback WHERE {LAST_HOUR} AND isp = :isp AND isp NOT IN ('att', :other)",
        {"isp": {"type": "string", "default": None}, "other": {"type": "string", "default": None}},
    )
    assert v.source.query(v.bind({}), NOW).filters == [Filter(dimension="isp", op="not_in", value=["att"])]
    assert v.source.query(v.bind({"isp": "cox", "other": "frontier"}), NOW).filters == [
        Filter(dimension="isp", op="=", value="cox"),
        Filter(dimension="isp", op="not_in", value=["att", "frontier"]),
    ]


def test_group_by_may_be_left_out_or_must_match_the_selected_dimensions():
    grouped = view(f"SELECT date_trunc('day', ts) AS day, isp, plays FROM playback WHERE {LAST_HOUR} GROUP BY 1, isp")
    implied = view(f"SELECT date_trunc('day', ts) AS day, isp, plays FROM playback WHERE {LAST_HOUR}")
    assert grouped.source.query({}, NOW) == implied.source.query({}, NOW)
    assert "GROUP BY must list exactly the selected dimensions and time bucket (isp, ts)" in error(
        f"SELECT date_trunc('day', ts) AS day, isp, plays FROM playback WHERE {LAST_HOUR} GROUP BY isp"
    )


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        (f"SELECT * FROM playback WHERE {LAST_HOUR}", "list the columns to select instead of *"),
        (f"SELECT count(*) AS n FROM playback WHERE {LAST_HOUR}", "aggregations are defined once, in ontology.yaml"),
        (f"SELECT latency_ms + 1 AS l, plays FROM playback WHERE {LAST_HOUR}", "can't select latency_ms + 1"),
        (f"SELECT nope, plays FROM playback WHERE {LAST_HOUR}", "'nope' isn't a metric or dimension of model 'playback'"),
        (f"SELECT revenue FROM playback WHERE {LAST_HOUR}", "'revenue' isn't a metric or dimension of model 'playback'"),
        (f"SELECT ts, plays FROM playback WHERE {LAST_HOUR}", "'ts' is the time dimension: select date_trunc('hour', ts)"),
        (f"SELECT date_trunc('quarter', ts) AS q, plays FROM playback WHERE {LAST_HOUR}", "date_trunc unit must be one of"),
        (f"SELECT isp FROM playback WHERE {LAST_HOUR}", "select at least one metric"),
        (f"SELECT plays, plays AS again FROM playback WHERE {LAST_HOUR}", "each metric or dimension can be selected once: plays"),
        ("SELECT plays FROM playback", "needs a time range: WHERE ts >= now() - INTERVAL '1' HOUR"),
        ("SELECT plays FROM playback WHERE ts > now() - INTERVAL '1' HOUR", "half-open"),
        ("SELECT plays FROM playback WHERE ts >= now() - INTERVAL '1' MONTH", "INTERVAL units for time ranges are"),
        ("SELECT plays FROM playback WHERE ts >= isp", "can't work out a time from isp"),
        ("SELECT plays FROM playback WHERE ts >= INTERVAL '1' HOUR", "isn't a time"),
        (f"SELECT plays FROM playback WHERE {LAST_HOUR} OR isp = 'att'", "can't use"),
        (f"SELECT plays FROM playback WHERE {LAST_HOUR} AND plays > 3", "can't filter on metric 'plays'"),
        (f"SELECT plays FROM playback WHERE {LAST_HOUR} AND device = 'ios'", "'device' isn't a dimension"),
        (f"SELECT plays FROM playback WHERE {LAST_HOUR} AND isp = 3", "'isp' is a string dimension"),
        (f"SELECT plays FROM playback WHERE {LAST_HOUR} AND isp = device", "compare 'isp' with literals or :parameters"),
        (f"SELECT isp, plays FROM playback WHERE {LAST_HOUR} GROUP BY isp HAVING plays > 1", "(it has having)"),
        (f"SELECT plays FROM playback JOIN gold.hourly ON true WHERE {LAST_HOUR}", "(it has joins)"),
        (f"SELECT DISTINCT isp, plays FROM playback WHERE {LAST_HOUR}", "(it has distinct)"),
        (f"SELECT plays FROM playback WHERE {LAST_HOUR} ORDER BY viewers", "ORDER BY: viewers isn't a selected column"),
        (f"SELECT plays FROM playback WHERE {LAST_HOUR} LIMIT 'ten'", "LIMIT takes a whole number"),
    ],
)
def test_model_view_mistakes_are_explained(sql, message):
    assert message in error(sql)


def test_a_time_range_that_ends_before_it_starts_fails_when_run():
    v = view("SELECT plays FROM playback WHERE ts >= now() AND ts < now() - INTERVAL :hours HOUR", {"hours": {"type": "int", "default": 1}})
    with pytest.raises(ViewError, match="start must be before end"):
        v.source.query(v.bind({}), NOW)


# ---------------------------------------------------------------- parameters


@pytest.mark.parametrize(
    ("param", "raw", "expected"),
    [
        ({"type": "int", "min": 1, "max": 48}, "12", 12),
        ({"type": "int", "min": 1, "max": 48}, 3.0, 3),  # JSON numbers from agents
        ({"type": "int", "min": 1, "max": 48}, "abc", "must be a whole number, not 'abc'"),
        ({"type": "int", "min": 1, "max": 48}, True, "must be a whole number"),
        ({"type": "int", "min": 1, "max": 48}, "0", "must be at least 1, not 0"),
        ({"type": "int", "min": 1, "max": 48}, 49, "must be at most 48, not 49"),
        ({"type": "float"}, "1.5", 1.5),
        ({"type": "float"}, "nan", "must be a number"),
        ({"type": "string", "values": ["comcast", "att"]}, "att", "att"),
        ({"type": "string", "values": ["comcast", "att"]}, "cox", "must be one of comcast, att, not 'cox'"),
        ({"type": "string"}, 5, "must be text"),
        ({"type": "boolean"}, "TRUE", True),
        ({"type": "boolean"}, "0", False),
        ({"type": "boolean"}, "maybe", "must be true or false"),
        ({"type": "timestamp"}, "2026-01-01T05:00:00+05:00", datetime(2026, 1, 1, tzinfo=UTC)),
        ({"type": "timestamp"}, "2026-01-01 05:00", datetime(2026, 1, 1, 5, tzinfo=UTC)),  # naive means UTC
        ({"type": "timestamp"}, "yesterday", "must be an ISO 8601 time"),
    ],
)
def test_parameters_are_converted_and_checked(param, raw, expected):
    sql = {
        "int": "SELECT plays FROM playback WHERE ts >= now() - INTERVAL :p HOUR",
        "float": "SELECT plays FROM playback WHERE ts >= now() - :p * INTERVAL '1' HOUR",
        "timestamp": "SELECT plays FROM playback WHERE ts >= :p",
        "boolean": f"SELECT plays FROM playback WHERE {LAST_HOUR} AND is_mobile = :p",
        "string": f"SELECT plays FROM playback WHERE {LAST_HOUR} AND isp = :p",
    }[param["type"]]
    v = view(sql, {"p": param})
    if isinstance(expected, str) and expected.startswith("must"):
        with pytest.raises(ViewError, match=re.escape(f"parameter 'p' {expected}")):
            v.bind({"p": raw})
    else:
        assert v.bind({"p": raw}) == {"p": expected}


def test_required_and_unknown_parameters():
    v = view("SELECT plays FROM playback WHERE ts >= now() - INTERVAL :hours HOUR", {"hours": {"type": "int"}})
    assert v.params["hours"].required
    with pytest.raises(ViewError, match=r"view 'test' needs parameter 'hours' \(int\)"):
        v.bind({})
    with pytest.raises(ViewError, match="view 'test' has no parameter 'hourz'; it takes hours"):
        v.bind({"hours": 1, "hourz": 2})


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("SELECT plays FROM playback", "start the file with a /* ... */ comment holding YAML"),
        ("/* params: {} */ SELECT plays FROM playback", "description: Field required"),
        ("/* description: [unclosed */ SELECT 1", "the header comment isn't valid YAML"),
        ("/*\ndescription: d\nparams: {h: {type: integer}}\n*/ SELECT 1 AS a", "params.h.type"),
        ("/*\ndescription: d\nparams: {h: {type: int, default: 5, max: 4}}\n*/ SELECT 1 AS a", "params.h: default must be at most 4, not 5"),
        ("/*\ndescription: d\nparams: {h: {type: string, min: 1}}\n*/ SELECT 1 AS a", "min and max only apply to int and float"),
        ("/*\ndescription: d\nparams: {h: {type: string, values: [a], default: b}}\n*/ SELECT 1 AS a", "must be one of a"),
        ("/*\ndescription: d\nparams: {include_sql: {type: boolean}}\n*/ SELECT 1 AS a", "reserved"),
        ("/* description: d */ SELECT plays FROM playback WHERE ts >= :since", ":since isn't declared under params"),
        ("/*\ndescription: d\nparams: {h: {type: int}}\n*/ SELECT 1 AS a", "declared parameters the SQL never uses: h"),
        ("/* description: d */ SELECT plays FROM playback WHERE ts >= ?", "write parameters by name, like :hours"),
        ("/* description: d */ SELECT 'oops", "invalid SQL"),
        ("/* description: d */ SELEC 1", "invalid SQL"),
    ],
)
def test_view_file_mistakes_are_explained(text, message):
    with pytest.raises(ViewError) as raised:
        parse_view("test", text, ONTOLOGY, TABLES)
    assert message in str(raised.value)


# ---------------------------------------------------------------- views over tables


def test_a_table_view_knows_its_columns_and_types():
    v = view(
        """
        WITH recent AS (SELECT isp, plays FROM gold.hourly WHERE ts >= :since)
        SELECT r.isp, sum(r.plays) AS plays, count(*) AS hours, max(p.ts) AS last_play
        FROM recent r JOIN raw.playback p ON p.isp = r.isp
        GROUP BY r.isp
        ORDER BY plays DESC
        """,
        {"since": {"type": "timestamp", "default": "2026-01-01T00:00:00"}},
    )
    assert v.reads == ["gold.hourly", "raw.playback"]
    assert [(c.name, c.type) for c in v.columns] == [
        ("isp", "string"),
        ("plays", "number"),
        ("hours", "number"),
        ("last_play", "timestamp"),
    ]
    sql = v.source.sql(v.bind({}), dialect="trino", catalog="iceberg", limit=100)
    assert "FROM iceberg.gold.hourly WHERE ts >= CAST('2026-01-01 00:00:00' AS TIMESTAMP)" in sql
    assert "JOIN iceberg.raw.playback AS p" in sql and "FROM recent AS r" in sql  # CTEs aren't tables
    assert sql.endswith("LIMIT 100")


def test_a_star_is_expanded_from_the_table_schema():
    assert [c.name for c in view("SELECT * FROM gold.hourly").columns] == ["ts", "isp", "plays", "slow_rate"]


def test_table_view_parameters_are_bound_as_literals_not_text():
    v = view("SELECT isp, plays FROM gold.hourly WHERE isp = :isp", {"isp": {"type": "string"}})
    attack = "x' OR '1'='1"
    sql = v.source.sql(v.bind({"isp": attack}), dialect="duckdb")
    [statement] = sqlglot.parse(sql, read="duckdb")
    assert statement.find(sqlglot.exp.EQ).expression.this == attack  # one comparison, with the whole value
    con = duckdb.connect()
    con.execute("CREATE SCHEMA gold; CREATE TABLE gold.hourly (ts TIMESTAMP, isp VARCHAR, plays BIGINT, slow_rate DOUBLE)")
    con.execute("INSERT INTO gold.hourly VALUES ('2026-01-01 00:00:00', 'att', 3, 0.1)")
    assert con.execute(sql).fetchall() == []


@pytest.mark.parametrize(
    ("sql", "params", "values", "limit"),
    [
        ("SELECT isp FROM gold.hourly LIMIT 5", None, {}, "LIMIT 5"),  # the view's own, lower limit stays
        ("SELECT isp FROM gold.hourly LIMIT 500", None, {}, "LIMIT 100"),
        ("SELECT isp FROM gold.hourly", None, {}, "LIMIT 100"),
        ("SELECT isp FROM gold.hourly LIMIT :n", {"n": {"type": "int", "default": None}}, {}, "LIMIT 100"),
        ("SELECT isp FROM gold.hourly LIMIT :n", {"n": {"type": "int", "default": None}}, {"n": 7}, "LIMIT 7"),
        ("SELECT isp FROM gold.hourly UNION ALL SELECT isp FROM raw.playback", None, {}, "LIMIT 100"),
    ],
)
def test_table_views_are_capped(sql, params, values, limit):
    v = view(sql, params)
    rendered = v.source.sql(v.bind(values), dialect="trino", limit=100)
    assert rendered.endswith(limit) and rendered.count("LIMIT") == 1


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        ("DELETE FROM gold.hourly", "a view is exactly one SELECT statement"),
        ("SELECT 1 AS a; DROP TABLE gold.hourly", "a view is exactly one SELECT statement"),
        ("SELECT * FROM gold.nope", "unknown table 'gold.nope'. Models: playback, billing. Tables: gold.hourly, raw.playback"),
        ("SELECT * FROM system.runtime.nodes", "without a catalog"),
        ("SELECT * FROM nodes", "unknown model or table 'nodes'"),
        ("SELECT * FROM TABLE(system.query(query => 'SELECT 1'))", "not table functions"),
        ("SELECT nope FROM gold.hourly", "Column 'nope' could not be resolved"),
        ("SELECT count(*) FROM gold.hourly", "name computed columns: COUNT(*) AS <name>"),
        ("SELECT isp, isp FROM gold.hourly", "column names must be unique: isp"),
        ("SELECT h.plays FROM gold.hourly AS h JOIN playback AS p ON TRUE", "must select from it alone"),
    ],
)
def test_table_view_mistakes_are_explained(sql, message):
    assert message in error(sql)


# ---------------------------------------------------------------- loading a folder


def test_load_views_checks_every_file_and_names_the_bad_ones(tmp_path):
    (tmp_path / "good.sql").write_text(f"/* description: Plays. */\nSELECT plays FROM playback WHERE {LAST_HOUR}")
    (tmp_path / "broken.sql").write_text("/* description: Oops. */\nSELECT nope FROM gold.hourly")
    (tmp_path / "Bad-Name.sql").write_text(f"/* description: Plays. */\nSELECT plays FROM playback WHERE {LAST_HOUR}")
    with pytest.raises(ViewError) as raised:
        load_views(tmp_path, ONTOLOGY, TABLES)
    message = str(raised.value)
    assert f"{tmp_path.name}/broken.sql: Column 'nope' could not be resolved" in message
    assert f"{tmp_path.name}/Bad-Name.sql: name views in lowercase snake_case" in message
    (tmp_path / "broken.sql").unlink()
    (tmp_path / "Bad-Name.sql").unlink()
    assert list(load_views(tmp_path, ONTOLOGY, TABLES)) == ["good"]
    assert load_views(tmp_path / "missing", ONTOLOGY, TABLES) == {}  # a project without views


def test_a_view_can_set_how_often_it_refreshes_live():
    assert view(f"SELECT plays FROM playback WHERE {LAST_HOUR}").refresh_seconds is None  # the server's default
    header = "/*\ndescription: d\nrefresh_seconds: 30\n*/\n"
    assert parse_view("test", header + f"SELECT plays FROM playback WHERE {LAST_HOUR}", ONTOLOGY, TABLES).refresh_seconds == 30
    with pytest.raises(ViewError, match="refresh_seconds: Input should be greater than or equal to 1"):
        parse_view("test", header.replace("30", "0.1") + "SELECT 1 AS a", ONTOLOGY, TABLES)
