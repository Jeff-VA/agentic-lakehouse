"""DuckDB end to end: the generated macros reading the gateway's answers as Parquet, in a real DuckDB.

The gateway is the stand-in, served over HTTP. These need GET /views/<name> and GET /sql to answer
format=parquet, with errors=inline (docs/design/sql-access.md); until then the module is skipped.
"""

from pathlib import Path

import duckdb
import pytest

from agentic_lakehouse.api_gateway import create_app
from agentic_lakehouse.browser import serving
from agentic_lakehouse.config import load_platform
from agentic_lakehouse.duckdb_client import render
from agentic_lakehouse.testing import standin_gateway

pytestmark = pytest.mark.skip(reason="needs the /sql endpoint and format=parquet (WS2); enabled when WS2 merges")

EXAMPLE = Path(__file__).parents[1] / "examples" / "playback" / "config"


@pytest.fixture(scope="module")
def con():
    """An in-memory DuckDB set up by the generated files, asking the stand-in gateway."""
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; INSTALL iceberg")  # a no-op once they're in DuckDB's folder
    except duckdb.Error as e:
        pytest.skip(f"DuckDB's httpfs and iceberg extensions aren't installed, and can't be downloaded here: {e}")
    platform = load_platform(EXAMPLE)
    with serving(create_app(standin_gateway(platform))) as url:
        files = render(platform, url, persistent_secret=False)  # a persistent secret would go into ~/.duckdb
        con.execute(files["setup.sql"])
        con.execute("\n".join(line for line in files["session.sql"].splitlines() if not line.startswith("ATTACH")))
        yield con
    con.close()


def ask(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[list[str], list[tuple]]:
    """The result's column names and rows."""
    result = con.execute(sql)
    return [column[0] for column in result.description], result.fetchall()


def test_a_view_over_a_model(con):
    columns, rows = ask(con, "SELECT * FROM gateway_view('isp_now', 'minutes=60')")
    assert columns == ["isp_provider", "rebuffer_incidence_rate", "p99_ttfb", "unique_sessions"]
    assert rows


def test_a_view_over_tables_with_its_defaults(con):
    columns, rows = ask(con, "SELECT * FROM gateway_view('isp_hourly', '')")
    assert columns == ["hour", "isp_provider", "rebuffer_incidence_rate", "p99_ttfb", "unique_sessions", "avg_bitrate_kbps"]
    assert rows


def test_sql_over_a_model_arrives_intact(con):
    """Quotes, spaces, <> and a newline in the SQL survive the trip through the URL."""
    columns, rows = ask(
        con,
        "SELECT * FROM lakehouse_sql('SELECT isp_provider, unique_sessions FROM playback_telemetry\n"
        "WHERE event_ts >= now() - INTERVAL ''1'' HOUR AND isp_provider <> ''nobody'' ORDER BY unique_sessions DESC LIMIT 3')",
    )
    assert columns == ["isp_provider", "unique_sessions"]
    assert 0 < len(rows) <= 3
    assert [row[1] for row in rows] == sorted((row[1] for row in rows), reverse=True)


def test_sql_over_tables_keeps_its_columns_when_empty(con):
    sql = "SELECT * FROM lakehouse_sql('SELECT isp_provider, p99_ttfb FROM gold.hourly_isp_quality WHERE isp_provider = ''nobody''')"
    assert ask(con, sql) == (["isp_provider", "p99_ttfb"], [])


def test_the_answers_are_tables_duckdb_can_join(con):
    columns, rows = ask(
        con,
        "SELECT v.isp_provider, v.unique_sessions, h.hours FROM gateway_view('isp_now', '') v "
        "JOIN lakehouse_sql('SELECT isp_provider, count(*) AS hours FROM gold.hourly_isp_quality GROUP BY isp_provider') h "
        "USING (isp_provider)",
    )
    assert columns == ["isp_provider", "unique_sessions", "hours"]
    assert rows


@pytest.mark.parametrize(
    "sql, mentions",
    [
        ("SELECT * FROM gateway_view('isp_now', 'minutes=1')", "minutes"),  # below the parameter's minimum
        ("SELECT * FROM gateway_view('isp_now', 'hours=1')", "hours"),  # a parameter it doesn't take
        ("SELECT * FROM gateway_view('nope', '')", "nope"),
        ("SELECT * FROM lakehouse_sql('SELECT nope FROM playback_telemetry WHERE event_ts >= now() - INTERVAL ''1'' HOUR')", "nope"),
        ("SELECT * FROM lakehouse_sql('DELETE FROM gold.hourly_isp_quality')", "SELECT"),
    ],
)
def test_a_mistake_comes_back_as_an_error_row(con, sql, mentions):
    columns, rows = ask(con, sql)
    assert columns == ["error"]
    assert len(rows) == 1 and mentions in rows[0][0]
