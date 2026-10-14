"""View results as CSV and Parquet files (agentic_lakehouse/formats.py)."""

import io
from datetime import UTC, date, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from agentic_lakehouse.formats import error_table, render, to_csv, to_parquet
from agentic_lakehouse.views import ViewColumn


def column(name: str, value_type: str) -> ViewColumn:
    return ViewColumn(name=name, type=value_type, label=name)


COLUMNS = [
    column("hour", "timestamp"),
    column("day", "date"),
    column("isp", "string"),
    column("plays", "number"),
    column("slow_rate", "number"),
    column("mobile", "boolean"),
    column("extra", "unknown"),
]
ROWS = [
    {
        "hour": datetime(2026, 1, 1, 2, tzinfo=UTC),
        "day": date(2026, 1, 1),
        "isp": "att",
        "plays": 3,
        "slow_rate": 0.25,
        "mobile": True,
        "extra": [1, 2],
    },
    {
        "hour": datetime(2026, 1, 1, 4, 30, tzinfo=timezone(timedelta(hours=2))),  # 02:30 UTC
        "day": None,
        "isp": "cox, inc",
        "plays": None,
        "slow_rate": 1,
        "mobile": False,
        "extra": None,
    },
]


def parquet(data: bytes) -> pa.Table:
    return pq.read_table(io.BytesIO(data))


def test_csv_has_a_header_then_rows_with_times_in_utc():
    assert to_csv(COLUMNS, ROWS).decode().splitlines() == [
        "hour,day,isp,plays,slow_rate,mobile,extra",
        "2026-01-01T02:00:00Z,2026-01-01,att,3,0.25,true,\"[1, 2]\"",
        '2026-01-01T02:30:00Z,,"cox, inc",,1,false,',  # a missing value is an empty field
    ]


def test_csv_keeps_its_header_without_rows():
    assert to_csv(COLUMNS, []) == b"hour,day,isp,plays,slow_rate,mobile,extra\r\n"


def test_parquet_columns_are_typed_from_the_view():
    table = parquet(to_parquet(COLUMNS, ROWS))
    assert table.schema == pa.schema(
        [
            ("hour", pa.timestamp("us", tz="UTC")),
            ("day", pa.date32()),
            ("isp", pa.string()),
            ("plays", pa.int64()),  # every value is whole
            ("slow_rate", pa.float64()),
            ("mobile", pa.bool_()),
            ("extra", pa.string()),  # unknown: the values as text, as in CSV
        ]
    )
    first, second = table.to_pylist()
    assert first == {
        "hour": datetime(2026, 1, 1, 2, tzinfo=UTC),
        "day": date(2026, 1, 1),
        "isp": "att",
        "plays": 3,
        "slow_rate": 0.25,
        "mobile": True,
        "extra": "[1, 2]",
    }
    assert (second["hour"], second["plays"], second["slow_rate"]) == (datetime(2026, 1, 1, 2, 30, tzinfo=UTC), None, 1.0)


def test_parquet_keeps_its_schema_without_rows():
    table = parquet(to_parquet(COLUMNS, []))
    assert table.num_rows == 0
    assert table.schema.names == [c.name for c in COLUMNS]
    assert table.schema.field("hour").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("plays").type == pa.float64()  # whole or not can't be told without values


@pytest.mark.parametrize(
    ("values", "arrow_type"),
    [([1, 2, None], pa.int64()), ([1, 2.5], pa.float64()), ([None, None], pa.float64())],
)
def test_parquet_numbers_are_whole_only_when_every_value_is(values, arrow_type):
    table = parquet(to_parquet([column("n", "number")], [{"n": v} for v in values]))
    assert table.schema.field("n").type == arrow_type


def test_naive_times_are_utc():
    columns, rows = [column("hour", "timestamp")], [{"hour": datetime(2026, 1, 1, 2)}]
    assert to_csv(columns, rows).decode().splitlines()[1] == "2026-01-01T02:00:00Z"
    assert parquet(to_parquet(columns, rows)).column("hour")[0].as_py() == datetime(2026, 1, 1, 2, tzinfo=UTC)


def test_an_error_is_one_row_in_one_string_column():
    columns, rows = error_table("no view named 'nope'")
    assert render("csv", columns, rows) == b"error\r\nno view named 'nope'\r\n"
    table = parquet(render("parquet", columns, rows))
    assert table.schema == pa.schema([("error", pa.string())])
    assert table.to_pylist() == [{"error": "no view named 'nope'"}]
