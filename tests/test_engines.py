import json
from datetime import datetime

import httpx
import pyarrow as pa
import pytest

from core.config import TrinoEndpoint
from core.engines import DuckDBEngine, EngineError, PinotEngine, TrinoEngine


def test_duckdb_returns_arrow():
    table = DuckDBEngine().execute("SELECT 1 AS n, 'a' AS s")
    assert table.to_pylist() == [{"n": 1, "s": "a"}]


def test_duckdb_errors_become_engine_errors():
    with pytest.raises(EngineError, match="duckdb"):
        DuckDBEngine().execute("SELECT * FROM missing_table")


def pinot(response: dict, requests: list | None = None) -> PinotEngine:
    """A PinotEngine whose broker always sends back `response`."""

    def handle(request: httpx.Request) -> httpx.Response:
        if requests is not None:
            requests.append(json.loads(request.content))
        return httpx.Response(200, json=response)

    return PinotEngine("http://broker", client=httpx.Client(transport=httpx.MockTransport(handle)))


def test_pinot_results_get_arrow_types():
    requests = []
    engine = pinot(
        {
            "resultTable": {
                "dataSchema": {
                    "columnNames": ["event_ts", "isp", "plays", "rate", "mobile"],
                    "columnDataTypes": ["TIMESTAMP", "STRING", "LONG", "DOUBLE", "BOOLEAN"],
                },
                "rows": [["2026-10-06 01:00:00.0", "cox", 7, 0.5, True]],
            },
            "exceptions": [],
        },
        requests,
    )
    table = engine.execute("SELECT 1")
    assert table.schema.types == [pa.timestamp("ms"), pa.string(), pa.int64(), pa.float64(), pa.bool_()]
    assert table.to_pylist() == [
        {"event_ts": datetime(2026, 10, 6, 1), "isp": "cox", "plays": 7, "rate": 0.5, "mobile": True}
    ]
    assert requests == [{"sql": "SELECT 1", "queryOptions": "useMultistageEngine=true;enableNullHandling=true"}]


def test_pinot_query_exceptions_become_engine_errors():
    engine = pinot({"exceptions": [{"errorCode": 150, "message": "SQLParsingError: bad"}]})
    with pytest.raises(EngineError, match="pinot: SQLParsingError: bad"):
        engine.execute("SELEC 1")


def test_pinot_nan_and_infinity_become_null():
    # PERCENTILETDIGEST over no rows answers "NaN"; DuckDB and Trino answer NULL.
    engine = pinot(
        {
            "resultTable": {
                "dataSchema": {"columnNames": ["p99"], "columnDataTypes": ["DOUBLE"]},
                "rows": [["NaN"], ["Infinity"], ["-Infinity"], [1.5]],
            },
            "exceptions": [],
        }
    )
    assert engine.execute("SELECT 1")["p99"].to_pylist() == [None, None, None, 1.5]


def test_pinot_values_that_dont_fit_their_type_are_engine_errors():
    engine = pinot(
        {"resultTable": {"dataSchema": {"columnNames": ["n"], "columnDataTypes": ["LONG"]}, "rows": [["x"]]}, "exceptions": []}
    )
    with pytest.raises(EngineError, match="pinot: column 'n'"):
        engine.execute("SELECT 1")


def test_unreachable_trino_is_an_engine_error():
    engine = TrinoEngine(TrinoEndpoint(host="127.0.0.1", port=1, user="test", catalog="iceberg"))
    with pytest.raises(EngineError, match="trino"):
        engine.execute("SELECT 1")
