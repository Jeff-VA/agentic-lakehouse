"""Drift signals (agentic_lakehouse/drift/signals.py) on Iceberg tables that real flushes write:
each publish() is a flush, which writes data files, so events fall into the windows by their
files' time ranges, as in production."""

import shutil
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest
import yaml
from pyiceberg.catalog.sql import SqlCatalog
from test_flush import T0, FakeConsumer, event

from agentic_lakehouse.config import Column, Platform, load_platform
from agentic_lakehouse.drift import signals
from agentic_lakehouse.drift.signals import detect_drift
from agentic_lakehouse.flush import NEWEST, flush
from agentic_lakehouse.schema_compiler import arrow_schema, iceberg_partition_spec, iceberg_schema

SAMPLE_CONFIG = Path(__file__).parents[1] / "examples" / "playback" / "config"
PLATFORM = load_platform(SAMPLE_CONFIG)
DATASET = PLATFORM.dataset("telemetry")
NOW = T0 + timedelta(days=8)  # the recent window is day 7 (hours 168 to 192), the earlier one days 0 to 6


def at(hours: float, drop: tuple[str, ...] = (), **fields) -> dict:
    """A telemetry event `hours` after T0, without the fields in `drop`."""
    sent = event(round(hours * 60), **fields)
    for name in drop:
        del sent[name]
    return sent


def every(start: float, count: int, step: float = 0.5, **fields) -> list[dict]:
    """`count` events, `step` hours apart from hour `start`."""
    return [at(start + i * step, **fields) for i in range(count)]


class Stream:
    """The telemetry topic and its Iceberg table: each publish() flushes the new events."""

    def __init__(self, table):
        self.table, self.published = table, []

    def publish(self, events: list[dict]) -> None:
        self.published += events
        flush(self.table, FakeConsumer(DATASET.topic, {0: list(self.published)}), DATASET, now=NOW, lateness=timedelta(0))


@pytest.fixture
def catalog(tmp_path):
    catalog = SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}")
    catalog.create_namespace("raw")
    return catalog


@pytest.fixture
def stream(catalog) -> Stream:
    partition_spec = iceberg_partition_spec(DATASET, PLATFORM.iceberg_table("telemetry"))
    return Stream(catalog.create_table("raw.telemetry", schema=iceberg_schema(DATASET), partition_spec=partition_spec))


def with_columns(*columns: Column) -> Platform:
    """The example platform with more columns in ingestion.yaml."""
    dataset = DATASET.model_copy(update={"columns": [*DATASET.columns, *columns]})
    return replace(PLATFORM, ingestion=PLATFORM.ingestion.model_copy(update={"datasets": [dataset]}))


def with_metric(tmp_path: Path, metric: dict, macros: str | None = None) -> Platform:
    """The example project with one more metric, `watched`, and a default.sql of project macros."""
    config = shutil.copytree(SAMPLE_CONFIG, tmp_path / "config")
    if macros:
        (config / "macros").mkdir()
        (config / "macros" / "default.sql").write_text(macros)
    ontology = yaml.safe_load((config / "ontology.yaml").read_text())
    ontology["models"][0]["metrics"].append({"name": "watched", "description": "Events over client_isp.", "agg": "count", **metric})
    (config / "ontology.yaml").write_text(yaml.safe_dump(ontology, sort_keys=False))
    return load_platform(config)


CDN_POP = with_columns(Column(name="cdn_pop", type="string"))


@pytest.fixture
def faded(stream) -> Stream:
    """A week of events, then a day without bitrate_kbps or cdn_pop. cdn_pop arrived halfway through
    the week: the flush added the column then, so the files before have no metrics for it."""
    stream.publish(every(1, 120))  # days 0 to 2
    stream.publish(every(72, 120, cdn_pop="den-1"))  # days 3 to 5
    stream.publish(every(169, 120, step=1 / 6, drop=("bitrate_kbps",)))  # day 7
    return stream


def test_columns_that_stop_arriving_fade_by_file_metrics(faded):
    report, _ = detect_drift("telemetry", CDN_POP, faded.table, now=NOW, min_recent_rows=100)

    assert [s.key for s in report.signals] == ["telemetry:fading_column:bitrate_kbps", "telemetry:fading_column:cdn_pop"]
    bitrate, cdn_pop = report.signals
    assert (bitrate.share_earlier, bitrate.share_recent) == (1.0, 0.0)
    assert (cdn_pop.share_earlier, cdn_pop.share_recent) == (0.5, 0.0)  # unset in the files written before the column
    assert bitrate.last_seen == cdn_pop.last_seen == T0 + timedelta(hours=131.5)  # the newest event in a file that set them
    assert bitrate.detail == (
        "column bitrate_kbps set on 0% of 120 events in the last 24 hours, 100% in the 7 days before; last set 2026-01-06 11:30"
    )
    assert (bitrate.read_by, cdn_pop.read_by) == (["metric playback_telemetry.avg_bitrate_kbps"], [])
    assert report.notes == []


def test_too_few_recent_events_to_judge_fading(faded):
    report, samples = detect_drift("telemetry", CDN_POP, faded.table, now=NOW)  # 120 recent events; 500 needed
    assert (report.signals, samples) == ([], None)
    assert report.notes == ["fading columns not judged: 120 events in the last 24 hours, fewer than 500"]


def test_windows_end_at_the_newest_event_not_the_wall_clock(faded):
    """Months after the last flush, the last day of events is still the recent window."""
    report, _ = detect_drift("telemetry", CDN_POP, faded.table, min_recent_rows=100)

    newest = T0 + timedelta(minutes=round((169 + 119 / 6) * 60))
    assert faded.table.properties[NEWEST] == newest.isoformat()
    end = newest + timedelta(microseconds=1)  # the newest event is in the recent window, [start, end)
    assert (report.earlier_start, report.window_start, report.window_end) == (end - timedelta(days=8), end - timedelta(days=1), end)
    assert [s.column for s in report.signals] == ["bitrate_kbps", "cdn_pop"]


def test_without_the_flushs_record_windows_end_at_the_newest_event_in_file_metrics(catalog):
    table = catalog.create_table("raw.telemetry", schema=iceberg_schema(DATASET))
    report, _ = detect_drift("telemetry", PLATFORM, table)  # no events at all: the wall clock
    assert abs(report.window_end - datetime.now(UTC).replace(tzinfo=None)) < timedelta(minutes=1)

    rows = [{**event(0), "event_ts": T0 + timedelta(hours=hours)} for hours in (1, 5, 3)]
    table.append(pa.Table.from_pylist(rows, schema=arrow_schema(DATASET)))  # written without a flush
    report, samples = detect_drift("telemetry", PLATFORM, table)
    assert report.window_end == T0 + timedelta(hours=5, microseconds=1)
    assert (report.signals, samples) == ([], None)

    with table.transaction() as transaction:  # the flush's record, when there is one, comes first
        transaction.set_properties({NEWEST: (T0 + timedelta(hours=4)).isoformat()})
    assert detect_drift("telemetry", PLATFORM, table)[0].window_end == T0 + timedelta(hours=4, microseconds=1)


@pytest.fixture
def relabeled(stream) -> Stream:
    """Four days of iOS and Android viewers on comcast and att, then a day with two new devices (tv on
    6 events, car on 3) and a new ISP (verizon, on those 9)."""
    stream.publish([at(1 + i, client_device=("ios", "android")[i % 2], client_isp=("comcast", "att")[i % 2]) for i in range(100)])
    day = [("tv", "verizon")] * 6 + [("car", "verizon")] * 3 + [("ios", "comcast")] * 10
    stream.publish([at(170 + i / 10, client_device=device, client_isp=isp) for i, (device, isp) in enumerate(day)])
    return stream


def test_new_values_are_watched_in_enumerated_columns(relabeled):
    report, _ = detect_drift("telemetry", PLATFORM, relabeled.table, now=NOW)

    # Not car: too few events. Not verizon: no definition compares client_isp with text; the
    # isp_provider dimension, which is just the column, shows it as a new group.
    [tv] = report.signals
    assert (tv.key, tv.kind, tv.column, tv.value, tv.rows) == ("telemetry:new_value:client_device=tv", "new_value", "client_device", "tv", 6)
    assert tv.detail == (
        "new value 'tv' in client_device: 6 events in the last 24 hours, none in the 7 days before; "
        "compared with 'ios', 'android' in dimension playback_telemetry.is_mobile_device"
    )
    assert tv.read_by == ["dimension playback_telemetry.is_mobile_device"]


def test_a_comparison_inside_a_project_macro_counts(relabeled, tmp_path):
    macros = "{# The ISPs most viewers use. #}\n{% macro big_isp() %}client_isp IN ('comcast', 'att'){% endmacro %}\n"
    platform = with_metric(tmp_path, {"filters": ["{{ big_isp() }}"]}, macros)
    report, _ = detect_drift("telemetry", platform, relabeled.table, now=NOW)

    assert [s.key for s in report.signals] == ["telemetry:new_value:client_isp=verizon", "telemetry:new_value:client_device=tv"]
    verizon = report.signals[0]
    assert verizon.rows == 9
    assert verizon.detail.endswith("compared with 'comcast', 'att' in metric playback_telemetry.watched")
    assert verizon.read_by == ["dimension playback_telemetry.isp_provider", "metric playback_telemetry.watched, via big_isp()"]


@pytest.mark.parametrize(
    ("metric", "compared"),
    [
        ({"filters": ["client_isp = 'comcast'"]}, "'comcast'"),
        ({"filters": ["'att' <> client_isp"]}, "'att'"),
        ({"agg": "sum", "sql": "CASE client_isp WHEN 'comcast' THEN 1 WHEN 'att' THEN 2 ELSE 0 END"}, "'comcast', 'att'"),
    ],
    ids=["equals", "not equals, reversed", "simple case"],
)
def test_comparisons_that_enumerate_a_column(relabeled, tmp_path, metric, compared):
    report, _ = detect_drift("telemetry", with_metric(tmp_path, metric), relabeled.table, now=NOW)
    assert report.signals[0].detail.endswith(f"compared with {compared} in metric playback_telemetry.watched")


def test_a_candidate_the_capped_read_missed_is_confirmed_by_a_targeted_scan(stream, monkeypatch):
    stream.publish([at(73 + i / 10, client_device="tv" if i >= 25 else "ios") for i in range(30)])  # tv after 25 ios
    stream.publish([at(170 + i / 10, client_device=device) for i, device in enumerate(["ios"] * 5 + ["tv"] * 5 + ["car"] * 5)])
    scanned, occurs = [], signals._occurs
    monkeypatch.setattr(signals, "_occurs", lambda table, column, value, rows: scanned.append(value) or occurs(table, column, value, rows))

    report, _ = detect_drift("telemetry", PLATFORM, stream.table, now=NOW, value_scan_limit=20)  # 20 earlier events: all ios
    assert [s.value for s in report.signals] == ["car"]  # tv is in the earlier window, after the 20
    assert sorted(scanned) == ["car", "tv"]
    scanned.clear()
    report, _ = detect_drift("telemetry", PLATFORM, stream.table, now=NOW)  # read whole, nothing to confirm
    assert ([s.value for s in report.signals], scanned) == (["car"], [])


def test_with_no_earlier_events_no_value_is_new(stream):
    stream.publish([at(170 + i / 10, client_device="tv") for i in range(10)])
    report, samples = detect_drift("telemetry", PLATFORM, stream.table, now=NOW)
    assert (report.signals, samples) == ([], None)
    assert "new values not judged: no events in the 7 days before the last 24 hours" in report.notes


def test_a_new_column_that_starts_as_a_fading_one_stops_may_replace_it(stream):
    stream.publish(every(1, 120))  # days 0 to 2
    stream.publish(every(169, 10, step=0.05))  # day 7, 01:00 to 01:27: client_isp and bitrate_kbps still sent
    renamed = {"drop": ("client_isp", "bitrate_kbps"), "isp": "comcast", "bitrate": 2500, "is_live": True}
    stream.publish(every(170, 110, step=0.1, **renamed))  # from 02:00 under new names, with a new flag
    stream.publish(every(185, 10, step=0.1, drop=("client_isp", "bitrate_kbps"), cdn_pop="den-1"))  # 17:00, unrelated

    report, _ = detect_drift("telemetry", PLATFORM, stream.table, now=NOW, min_recent_rows=100)
    by_column = {s.column: s for s in report.signals}
    replaces = {s.column: s.replaces for s in report.signals if s.kind == "new_column"}
    # bitrate is a double, as the flush makes every new number, and still may replace the int bitrate_kbps.
    # is_live is a boolean, and cdn_pop arrived 15 hours later.
    assert replaces == {"isp": "client_isp", "bitrate": "bitrate_kbps", "is_live": None, "cdn_pop": None}
    assert by_column["isp"].detail.endswith(
        "; first set 2026-01-08 02:00, near when column client_isp was last set (2026-01-08 01:27): it may replace client_isp"
    )
    assert by_column["client_isp"].detail == (
        "column client_isp set on 8% of 130 events in the last 24 hours, 100% in the 7 days before; "
        "last set 2026-01-08 01:27; new column isp, first set 2026-01-08 02:00, may replace it"
    )


def test_the_samples_hold_rows_of_each_window_and_for_each_signal(stream):
    stream.publish(every(1, 120) + every(73, 120, bitrate_kbps=None))  # days 0 to 5; bitrate_kbps on the first half
    day = []
    for i in range(150):  # day 7: no bitrate_kbps; a new device on 6 events, and a new field on 3 of those
        fields = {"client_device": "tv" if i % 25 == 24 else "ios"} | ({"cdn_pop": "den-1"} if i % 50 == 49 else {})
        day.append(at(170 + i / 10, drop=("bitrate_kbps",), **fields))
    stream.publish(day)

    report, samples = detect_drift("telemetry", PLATFORM, stream.table, 10, now=NOW, min_recent_rows=100)
    assert [s.key for s in report.signals] == [
        "telemetry:new_column:cdn_pop",
        "telemetry:fading_column:bitrate_kbps",
        "telemetry:new_value:client_device=tv",
    ]
    recent, earlier = samples.recent.to_pylist(), samples.earlier.to_pylist()
    assert samples.recent.column_names == samples.earlier.column_names == [f.name for f in stream.table.schema().fields]
    assert all(report.window_start <= row["event_ts"] < report.window_end for row in recent)
    assert all(report.earlier_start <= row["event_ts"] < report.window_start for row in earlier)
    for rows in (recent, earlier):
        assert len({row["session_id"] for row in rows}) == len(rows)  # no row twice
    # The window's first 10 events, then the 3 where the new column is set, then the 3 other tv events
    assert len(recent) == 10 + 3 + 3
    assert [row["cdn_pop"] for row in recent[10:13]] == ["den-1"] * 3
    assert [row["client_device"] for row in recent[10:]] == ["tv"] * 6
    # 10 of the earlier window's events, and all 120 where the fading column was set (some may be among the 10)
    assert sum(row["bitrate_kbps"] is not None for row in earlier) == 120
    assert 120 <= len(earlier) <= 130
