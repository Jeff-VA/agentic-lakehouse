"""Results as files, and SQL without a view file: format= and errors= on GET /views/{name} and /sql,
HEAD, GET and POST /sql, and the MCP tool query_sql. On the stand-in gateway over the example
project, and with a real DuckDB reading the gateway over HTTP."""

import csv
import io
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient
from test_api_gateway import call_mcp

from agentic_lakehouse.api_gateway import create_app, create_mcp_server
from agentic_lakehouse.browser import serving
from agentic_lakehouse.config import load_platform
from agentic_lakehouse.testing import standin_gateway
from agentic_lakehouse.views import ViewError

EXAMPLE = Path(__file__).parents[1] / "examples" / "playback" / "config"
# A complete hour inside the stand-in's six hours of events, fixed so that every request asks the same question.
START = (datetime.now(UTC) - timedelta(hours=3)).replace(minute=0, second=0, microsecond=0, tzinfo=None)
END = START + timedelta(hours=1)
BETWEEN = f"event_ts >= TIMESTAMP '{START}' AND event_ts < TIMESTAMP '{END}'"
MODEL_SQL = f"""SELECT isp_provider, rebuffer_incidence_rate, unique_sessions
FROM playback_telemetry
WHERE {BETWEEN}
ORDER BY isp_provider"""
TABLE_SQL = f"""SELECT event_ts AS hour, isp_provider, unique_sessions
FROM gold.hourly_isp_quality
WHERE {BETWEEN}
ORDER BY isp_provider"""
HEADER_SQL = """/*
description: Sessions per ISP, from the gold rollup.
params:
  isp: {type: string, default: att}
*/
SELECT isp_provider, sum(unique_sessions) AS sessions
FROM gold.hourly_isp_quality
WHERE isp_provider = :isp
GROUP BY isp_provider"""
ENGINE_FAILS = "SELECT CAST(isp_provider AS INTEGER) AS n FROM gold.hourly_isp_quality"  # valid SQL; DuckDB can't cast 'att'


@pytest.fixture(scope="module")
def gateway():
    return standin_gateway(load_platform(EXAMPLE))


@pytest.fixture(scope="module")
def client(gateway):
    with TestClient(create_app(gateway)) as client:
        yield client


@pytest.fixture
def frozen(gateway, monkeypatch):
    """The router's clock stopped, so a view over a model answers each request alike."""
    now = datetime.now(UTC)
    monkeypatch.setattr(gateway.router, "clock", lambda: now)


def parquet(response) -> pa.Table:
    assert (response.status_code, response.headers["content-type"]) == (200, "application/vnd.apache.parquet")
    return pq.read_table(io.BytesIO(response.content))


def csv_rows(response) -> list[dict[str, str]]:
    assert (response.status_code, response.headers["content-type"]) == (200, "text/csv; charset=utf-8")
    return list(csv.DictReader(io.StringIO(response.text)))


# ---------------------------------------------------------------- formats


def test_a_view_comes_as_csv_or_parquet_with_the_rows_json_has(client, frozen):
    params = {"minutes": 30}
    rows = client.get("/views/isp_now", params=params).json()["rows"]
    assert len(rows) == 6  # one per ISP
    table = parquet(client.get("/views/isp_now", params=params | {"format": "parquet"}))
    assert table.schema == pa.schema(
        [
            ("isp_provider", pa.string()),
            ("rebuffer_incidence_rate", pa.float64()),
            ("p99_ttfb", pa.float64()),
            ("unique_sessions", pa.int64()),
        ]
    )
    assert table.to_pylist() == rows
    text_rows = csv_rows(client.get("/views/isp_now", params=params | {"format": "csv", "include_sql": True}))
    assert text_rows == [{key: str(value) for key, value in row.items()} for row in rows]


def test_files_write_times_in_utc(client):
    [row] = csv_rows(client.get("/sql", params={"q": TABLE_SQL + " LIMIT 1", "format": "csv"}))
    assert row["hour"] == START.isoformat() + "Z"
    table = parquet(client.get("/sql", params={"q": TABLE_SQL, "format": "parquet"}))
    assert table.schema.field("hour").type == pa.timestamp("us", tz="UTC")
    assert table.column("hour")[0].as_py() == START.replace(tzinfo=UTC)


@pytest.mark.parametrize(
    ("sql", "schema"),
    [
        (
            f"SELECT isp_provider, unique_sessions FROM playback_telemetry WHERE {BETWEEN} AND isp_provider = 'nobody'",
            [("isp_provider", pa.string()), ("unique_sessions", pa.float64())],  # without values, a number is float64
        ),
        (
            "SELECT event_ts AS hour, isp_provider FROM gold.hourly_isp_quality WHERE isp_provider = 'nobody'",
            [("hour", pa.timestamp("us", tz="UTC")), ("isp_provider", pa.string())],
        ),
    ],
)
def test_empty_results_keep_their_columns(client, sql, schema):
    assert client.get("/sql", params={"q": sql}).json()["rows"] == []
    response = client.get("/sql", params={"q": sql, "format": "csv"})
    assert response.text == ",".join(name for name, _ in schema) + "\r\n"
    assert parquet(client.get("/sql", params={"q": sql, "format": "parquet"})).schema == pa.schema(schema)


def test_files_hold_up_to_export_max_rows(client, gateway, monkeypatch):
    monkeypatch.setattr(gateway, "max_rows", 2)
    monkeypatch.setattr(gateway, "export_max_rows", 4)
    body = client.get("/sql", params={"q": TABLE_SQL}).json()
    assert (len(body["rows"]), body["truncated"]) == (2, True)
    assert len(csv_rows(client.get("/sql", params={"q": TABLE_SQL, "format": "csv"}))) == 4
    assert parquet(client.get("/sql", params={"q": TABLE_SQL, "format": "parquet"})).num_rows == 4


# ---------------------------------------------------------------- errors


@pytest.mark.parametrize("file_format", ["csv", "parquet"])
@pytest.mark.parametrize(
    ("path", "params", "message"),
    [
        ("/views/isp_now", {"minutes": "abc"}, "parameter 'minutes' must be a whole number, not 'abc'"),
        ("/views/nope", {}, "no view named 'nope'"),
        ("/sql", {"q": "SELECT nope FROM nowhere"}, "unknown model or table 'nowhere'. Models: playback_telemetry."),
        ("/sql", {"q": ENGINE_FAILS}, "duckdb: Conversion Error: Could not convert string 'att'"),
    ],
)
def test_inline_errors_are_a_row_in_column_error(client, file_format, path, params, message):
    response = client.get(path, params=params | {"format": file_format, "errors": "inline"})
    [row] = csv_rows(response) if file_format == "csv" else parquet(response).to_pylist()
    assert list(row) == ["error"]
    assert message in row["error"]


def test_posted_sql_errors_inline_too(client):
    response = client.post("/sql", params={"format": "csv", "errors": "inline"}, json={"sql": "SELECT nope FROM nowhere"})
    assert "unknown model or table 'nowhere'" in csv_rows(response)[0]["error"]


@pytest.mark.parametrize(
    ("path", "params", "status", "detail"),
    [
        ("/views/isp_now", {"format": "csv", "minutes": "abc"}, 400, "parameter 'minutes' must be a whole number, not 'abc'"),
        ("/views/nope", {"format": "parquet"}, 404, "no view named 'nope'"),
        ("/views/nope", {"errors": "inline"}, 404, "no view named 'nope'"),  # JSON keeps real status codes
        ("/sql", {"q": ENGINE_FAILS, "format": "csv"}, 502, "duckdb: Conversion Error"),
    ],
)
def test_without_inline_errors_files_get_status_codes(client, path, params, status, detail):
    response = client.get(path, params=params)
    assert response.status_code == status
    assert detail in response.json()["detail"]


def test_head_answers_without_running_anything(client, gateway, monkeypatch):
    def run_view(*args, **kwargs):
        raise AssertionError("HEAD ran the query")

    monkeypatch.setattr(gateway, "run_view", run_view)
    for path, content_type in [
        ("/views", "application/json"),
        ("/views/isp_now", "application/json"),
        ("/views/isp_now?format=csv&minutes=30", "text/csv; charset=utf-8"),
        ("/views/isp_now?format=parquet", "application/vnd.apache.parquet"),
        ("/views/nope?format=parquet&errors=inline", "application/vnd.apache.parquet"),  # GET answers with the error row
        ("/sql?q=SELECT%201&format=csv", "text/csv; charset=utf-8"),
    ]:
        response = client.head(path)
        assert (response.status_code, response.headers["content-type"], response.content) == (200, content_type, b""), path
        assert "content-length" not in response.headers  # knowing it would take running the query
    assert client.head("/views/nope?format=parquet").status_code == 404
    assert client.head("/sql").status_code == 422  # no q


# ---------------------------------------------------------------- /sql


def test_sql_over_a_model_answers_like_the_query_it_stands_for(client):
    body = client.get("/sql", params={"q": MODEL_SQL, "include_sql": True}).json()
    query = client.post(
        "/query",
        json={
            "metrics": ["rebuffer_incidence_rate", "unique_sessions"],
            "dimensions": ["isp_provider"],
            "start": f"{START.isoformat()}Z",
            "end": f"{END.isoformat()}Z",
            "order_by": [{"field": "isp_provider"}],
        },
    ).json()
    assert len(body["rows"]) == 6 and body["rows"] == query["rows"]
    assert (body["view"], body["params"], body["coverage"], body["data_through"]) == ("sql", {}, query["coverage"], None)
    assert body["sql"]  # what each engine ran


def test_sql_over_tables_runs_on_the_warehouse(client):
    body = client.get("/sql", params={"q": TABLE_SQL}).json()
    assert [row["isp_provider"] for row in body["rows"]] == ["att", "comcast", "cox", "frontier", "spectrum", "verizon"]
    assert body["coverage"] is None and body["data_through"] is not None


def test_sql_can_be_posted(client):
    get = client.get("/sql", params={"q": TABLE_SQL}).json()
    post = client.post("/sql", json={"sql": TABLE_SQL}).json()
    assert post["rows"] == get["rows"]
    assert parquet(client.post("/sql", params={"format": "parquet"}, json={"sql": TABLE_SQL})).num_rows == len(get["rows"])


def test_sql_with_a_header_takes_parameters(client):
    get = client.get("/sql", params={"q": HEADER_SQL, "isp": "cox"}).json()
    post = client.post("/sql", json={"sql": HEADER_SQL, "params": {"isp": "cox"}}).json()
    assert get["params"] == post["params"] == {"isp": "cox"}
    assert [row["isp_provider"] for row in get["rows"]] == ["cox"] and post["rows"] == get["rows"]
    assert client.get("/sql", params={"q": HEADER_SQL}).json()["rows"][0]["isp_provider"] == "att"  # the default


@pytest.mark.parametrize(
    ("sql", "params", "detail"),
    [
        ("SELECT isp_provider FROM gold.hourly_isp_quality WHERE isp_provider = :isp", {}, ":isp isn't declared under params"),
        ("SELECT isp_provider FROM gold.hourly_isp_quality", {"isp": "att"}, "view 'sql' has no parameter 'isp'; it takes none"),
        ("DELETE FROM gold.hourly_isp_quality", {}, "a view is exactly one SELECT statement"),
        ("SELECT p99_ttfb FROM playback_telemetry", {}, "needs a time range"),
    ],
)
def test_bad_sql_gets_a_client_error(client, sql, params, detail):
    response = client.get("/sql", params={"q": sql} | params)
    assert response.status_code == 400
    assert detail in response.json()["detail"]


@pytest.mark.parametrize("name", ["format", "errors", "q", "include_sql"])
def test_views_cant_take_parameters_named_like_the_gateways_options(gateway, name):
    with pytest.raises(ViewError, match=f"parameter names {name} are reserved"):
        gateway.parse_sql(HEADER_SQL.replace("isp:", f"{name}:").replace(":isp", f":{name}"))


def test_mcp_runs_sql_like_rest(gateway, client):
    server = create_mcp_server(gateway)
    tools = {t.name: t for t in call_mcp(server, "list_tools").tools}
    assert tools["query_sql"].annotations.read_only_hint is True
    result = call_mcp(server, "call_tool", "query_sql", {"sql": MODEL_SQL})
    assert result.is_error is False
    assert result.structured_content["rows"] == client.get("/sql", params={"q": MODEL_SQL}).json()["rows"]
    failed = call_mcp(server, "call_tool", "query_sql", {"sql": "SELECT nope FROM nowhere"})
    assert failed.is_error is True
    assert "unknown model or table 'nowhere'" in failed.content[0].text


# ---------------------------------------------------------------- DuckDB over HTTP


def duckdb_over_http() -> duckdb.DuckDBPyConnection:
    """DuckDB with its httpfs extension loaded, or a skip when the extension can't be had (offline, say)."""
    con = duckdb.connect()
    try:
        con.execute("LOAD httpfs")
    except duckdb.Error:
        try:
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")
        except duckdb.Error as e:
            pytest.skip(f"DuckDB's httpfs extension isn't available: {e}")
    con.execute("SET TimeZone = 'UTC'")
    return con


def test_duckdb_reads_each_result_with_one_get(gateway):
    """The gateway answers DuckDB's HEAD without a Content-Length, so DuckDB reads the result with one
    plain GET: sessions don't need force_download, and no byte-range requests run the query again."""
    con = duckdb_over_http()
    app = create_app(gateway)
    requests = []

    async def recorded(scope, receive, send):
        if scope["type"] == "http":
            requests.append((scope["method"], scope["path"], dict(scope["headers"]).get(b"range")))
        await app(scope, receive, send)

    def read(reader: str, path: str, *, force_download: bool = False) -> tuple[dict[str, str], list[tuple]]:
        """The columns (name -> DuckDB type) and rows of SELECT * FROM `reader`, which reads `path` once."""
        con.execute(f"SET force_download = {force_download}")
        requests.clear()
        cursor = con.execute(f"SELECT * FROM {reader}")
        rows = cursor.fetchall()
        assert requests == ([] if force_download else [("HEAD", path, None)]) + [("GET", path, None)]
        return {name: str(column_type) for name, column_type, *_ in cursor.description}, rows

    with serving(recorded) as url:
        columns, rows = read(f"read_parquet('{url}/views/isp_hourly?format=parquet&hours=6')", "/views/isp_hourly")
        assert columns == {
            "hour": "TIMESTAMP WITH TIME ZONE",
            "isp_provider": "VARCHAR",
            "rebuffer_incidence_rate": "DOUBLE",
            "p99_ttfb": "DOUBLE",
            "unique_sessions": "BIGINT",
            "avg_bitrate_kbps": "DOUBLE",
        }
        assert rows

        # Ad-hoc SQL, URL-encoded by DuckDB, as a session's macros send it
        _, rows = read(f"read_parquet('{url}/sql?format=parquet&errors=inline&q=' || url_encode($q${MODEL_SQL}$q$))", "/sql")
        assert rows == [tuple(row.values()) for row in gateway.run_view(gateway.parse_sql(MODEL_SQL)).rows]
        text = "a+b & c=d #1 100% é"  # what a query string would otherwise take apart
        tricky = f"SELECT unique_sessions + 1 AS more, '{text}' AS text FROM gold.hourly_isp_quality LIMIT 1"
        _, rows = read(f"read_parquet('{url}/sql?format=parquet&q=' || url_encode($q${tricky}$q$))", "/sql")
        assert rows[0][1] == text

        columns, rows = read(f"read_parquet('{url}/sql?format=parquet&q=' || url_encode($q${TABLE_SQL} LIMIT 0$q$))", "/sql")
        assert (list(columns), rows) == (["hour", "isp_provider", "unique_sessions"], [])

        columns, rows = read(f"read_csv('{url}/views/isp_now?format=csv&errors=inline&minutes=abc')", "/views/isp_now")
        assert (columns, rows) == ({"error": "VARCHAR"}, [("parameter 'minutes' must be a whole number, not 'abc'",)])

        # force_download skips the HEAD; it works as well, with one request
        _, rows = read(f"read_parquet('{url}/views/isp_now?format=parquet')", "/views/isp_now", force_download=True)
        assert len(rows) == 6
