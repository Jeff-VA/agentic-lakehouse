"""Composed views (docs/design/sql-access.md): semantic queries and table queries answered in
full, then SQL over their results in a sandboxed DuckDB."""

import asyncio

import pytest
from test_compiler import ONTOLOGY
from test_live import REFRESH, Watch
from test_router import router
from test_view_api import VIEWS, warehouse
from test_views import TABLES

from agentic_lakehouse import compose
from agentic_lakehouse.api_gateway import Gateway
from agentic_lakehouse.live import LiveViews
from agentic_lakehouse.views import ComposedSource, ModelSource, TableSource, ViewError, load_views, parse_view

GAINERS = """/*
description: Each ISP's slow share in the last `hours` against the last 6 hours.
params:
  hours: {type: int, default: 1, min: 1, max: 6}
  min_plays: {type: int, default: 0}
columns:
  change: {label: Change, format: percent, better: lower}
*/
WITH recent AS (
  SELECT isp, plays, slow_rate FROM playback WHERE ts >= now() - INTERVAL :hours HOUR
),
usual AS (
  SELECT isp, slow_rate AS usual_rate FROM playback WHERE ts >= now() - INTERVAL '6' HOUR
)
SELECT recent.isp, recent.plays, recent.slow_rate, usual.usual_rate, recent.slow_rate - usual.usual_rate AS change
FROM recent JOIN usual ON usual.isp = recent.isp
WHERE recent.plays >= :min_plays
ORDER BY change DESC, recent.isp
"""

WITH_GOLD = """/*
description: Plays per ISP in the last 6 hours, ranked, with each ISP's busiest hour in the gold table.
*/
WITH live AS (
  SELECT isp, plays FROM playback WHERE ts >= now() - INTERVAL '6' HOUR
),
hourly AS (
  SELECT isp, max(plays) AS busiest_hour FROM gold.hourly GROUP BY isp
),
ranked AS (
  SELECT live.isp, live.plays, hourly.busiest_hour, rank() OVER (ORDER BY live.plays DESC) AS place
  FROM live LEFT JOIN hourly ON hourly.isp = live.isp
)
SELECT * FROM ranked ORDER BY place, isp
"""

RECENT = "recent AS (SELECT isp, plays FROM playback WHERE ts >= now() - INTERVAL '1' HOUR)"


def view(sql: str, header: str = "/*\ndescription: A test view.\n*/\n"):
    return parse_view("test", header + sql, ONTOLOGY, TABLES)


@pytest.fixture
def gateway(tmp_path) -> Gateway:
    for name, text in (VIEWS | {"gainers": GAINERS, "with_gold": WITH_GOLD}).items():
        (tmp_path / f"{name}.sql").write_text(text)
    return Gateway(router(), views=load_views(tmp_path, ONTOLOGY, TABLES), warehouse=warehouse())


# ---------------------------------------------------------------- what a view is


def test_a_with_query_that_reads_a_model_is_composed():
    v = parse_view("gainers", GAINERS, ONTOLOGY, TABLES)
    assert isinstance(v.source, ComposedSource)
    assert {name: type(part) for name, part in v.source.parts.items()} == {"recent": ModelSource, "usual": ModelSource}
    assert (v.reads, v.realtime, v.tables) == (["playback"], True, [])
    columns = {c.name: c for c in v.columns}
    assert [(c.name, c.type) for c in v.columns] == [
        ("isp", "string"), ("plays", "number"), ("slow_rate", "number"), ("usual_rate", "number"), ("change", "number"),
    ]
    assert (columns["plays"].format, columns["plays"].role) == ("integer", "metric")  # a count reads as a whole number
    assert (columns["change"].label, columns["change"].format, columns["change"].better) == ("Change", "percent", "lower")


def test_views_keep_their_kind_without_a_model_in_a_with():
    assert isinstance(view("SELECT isp, plays FROM playback WHERE ts >= now() - INTERVAL '1' HOUR").source, ModelSource)
    over_tables = view("WITH t AS (SELECT isp, plays FROM gold.hourly) SELECT isp, sum(plays) AS plays FROM t GROUP BY isp")
    assert isinstance(over_tables.source, TableSource)  # a table view, run on Trino as written


def test_a_model_part_and_a_table_part():
    v = parse_view("with_gold", WITH_GOLD, ONTOLOGY, TABLES)
    assert {name: type(part) for name, part in v.source.parts.items()} == {"live": ModelSource, "hourly": TableSource}
    assert (v.reads, v.realtime, v.tables) == (["playback", "gold.hourly"], True, ["gold.hourly"])
    assert [(c.name, c.type) for c in v.columns] == [("isp", "string"), ("plays", "number"), ("busiest_hour", "number"), ("place", "number")]


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        (
            "WITH x AS (SELECT p.isp FROM playback AS p JOIN gold.hourly AS h ON h.isp = p.isp) SELECT * FROM x",
            "CTE x reads gold.hourly and playback: a CTE reads one model, or tables, or other CTEs. Split it into CTEs and join them",
        ),
        (
            f"WITH {RECENT}, b AS (SELECT r.isp, plays FROM playback JOIN recent AS r ON r.isp = playback.isp) SELECT * FROM b",
            "CTE b reads playback and recent: a CTE reads one model, or tables, or other CTEs",
        ),
        (
            "WITH x AS (SELECT p.isp FROM playback AS p, billing AS b) SELECT * FROM x",
            "CTE x reads billing and playback: read each model in a CTE of its own",
        ),
        (
            f"WITH {RECENT} SELECT recent.isp FROM recent JOIN gold.hourly AS h ON h.isp = recent.isp",
            "after the WITH, read only the CTEs; read the table gold.hourly in a CTE of its own",
        ),
        (f"WITH {RECENT} SELECT * FROM playback", "after the WITH, read only the CTEs; read the model playback in a CTE of its own"),
        (f"WITH {RECENT} SELECT * FROM recent, read_csv('/etc/hosts')", "after the WITH, read the CTEs, not table functions"),
        (f"WITH {RECENT} SELECT no_such_function(isp) AS x FROM recent", "after the WITH: Catalog Error: Scalar Function with name no_such_function"),
        (f"WITH {RECENT}, b AS (SELECT * FROM nowhere) SELECT * FROM recent", "CTE b reads 'nowhere', which is no model, table or earlier CTE"),
        ("WITH a AS (SELECT isp, nope FROM playback WHERE ts >= now() - INTERVAL '1' HOUR) SELECT * FROM a", "CTE a: "),
        ("WITH playback AS (SELECT isp, plays FROM playback WHERE ts >= now() - INTERVAL '1' HOUR) SELECT * FROM playback",
         "name the CTE playback something other than the model it shares a name with"),
        (f"WITH RECURSIVE {RECENT} SELECT * FROM recent", "composed views can't use WITH RECURSIVE"),
        (f"WITH {RECENT} SELECT isp, isp FROM recent", "column names must be unique: isp"),
        (
            "SELECT p.isp FROM playback AS p JOIN gold.hourly AS h ON h.isp = p.isp",
            "it has joins). To combine it with other data, read it in a WITH clause and join the CTE",
        ),
    ],
)
def test_composed_view_mistakes_are_explained(sql, message):
    with pytest.raises(ViewError) as raised:
        view(sql)
    assert message in str(raised.value)


# ---------------------------------------------------------------- answering it


def test_stage_two_works_on_the_merged_answers_of_its_parts(gateway):
    result = gateway.view("gainers", {"hours": 2})
    by_isp = {row["isp"]: row for row in result.rows}
    recent = {row["isp"]: row for row in gateway.view("plays_by_isp", {"hours": 2}).rows}
    usual = {row["isp"]: row for row in gateway.view("plays_by_isp", {"hours": 6}).rows}
    assert by_isp and by_isp.keys() <= recent.keys()
    for isp, row in by_isp.items():  # each part is exactly what its own query answers
        assert row["plays"] == recent[isp]["plays"]
        assert row["slow_rate"] == pytest.approx(recent[isp]["slow"])
        assert row["usual_rate"] == pytest.approx(usual[isp]["slow"])
        assert row["change"] == pytest.approx(recent[isp]["slow"] - usual[isp]["slow"])
    assert [r["change"] for r in result.rows] == sorted((r["change"] for r in result.rows), reverse=True)
    assert result.parts.keys() == {"recent", "usual"} and all(p.source == "model" for p in result.parts.values())
    assert result.coverage is not None and result.data_through is None  # live, like a view over a model
    assert gateway.view("gainers", {"hours": 2, "min_plays": 10**6}).rows == []  # a parameter in stage 2


def test_a_part_over_tables_brings_its_freshness(gateway):
    result = gateway.view("with_gold", include_sql=True)
    rows = {row["isp"]: row for row in result.rows}
    assert rows["comcast"]["busiest_hour"] == 8 and rows["att"]["busiest_hour"] == 3  # the gold table's maxima
    assert rows["verizon"]["busiest_hour"] is None  # no gold rows: the LEFT JOIN keeps it
    assert [r["place"] for r in result.rows] == sorted(r["place"] for r in result.rows)
    assert result.parts["hourly"].source == "tables" and result.parts["hourly"].data_through is not None
    assert result.data_through == result.parts["hourly"].data_through
    assert "stage 2" in result.sql and any(key.startswith("hourly: ") for key in result.sql)


def test_parts_too_large_to_combine_fail_and_long_results_are_cut(gateway):
    v = gateway.views["gainers"]
    cut = gateway.run_view(v, {"hours": 6}, max_rows=1)
    assert len(cut.rows) == 1 and cut.truncated and "Only the first 1 rows are returned" in cut.notes[-1]
    gateway.part_max_rows = 1
    with pytest.raises(ViewError, match="part recent of view 'gainers' has more than 1 rows, too many to combine"):
        gateway.view("gainers", {"hours": 6})


def test_a_composed_view_is_listed_as_composed(gateway):
    info = {v.name: v for v in gateway.view_catalog().views}
    assert (info["with_gold"].source, info["with_gold"].reads) == ("composed", ["playback", "gold.hourly"])


def test_a_live_composed_view_pushes_only_changes(gateway):
    live = LiveViews(gateway, refresh_seconds=REFRESH)

    async def main():
        watch = Watch(live, "gainers")
        await watch.until(1)
        await asyncio.sleep(10 * REFRESH)
        assert len(watch.events) == 1  # rerun every refresh; the parts' moving time ranges aren't a change
        await watch.stop()

    asyncio.run(main())


# ---------------------------------------------------------------- the sandbox


def test_stage_two_cannot_reach_files_or_run_forever():
    with pytest.raises(compose.StageTwoError, match="Permission Error"):
        compose.run({}, "SELECT * FROM read_csv('/etc/hosts')")
    with pytest.raises(compose.StageTwoError, match="took longer than 0.2 seconds"):
        compose.run({}, "SELECT sum(i) FROM range(100000000000) AS t(i)", timeout=0.2)
