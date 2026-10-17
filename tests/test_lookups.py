"""Lookups (the ontology's top-level `lookups`): a small table keyed by a dimension, whose attributes
become dimensions of every model with that dimension. Each compiles into the query as a CASE over the
key, so it's exact on every tier and for every aggregation, with no joins."""

import copy
from dataclasses import replace

import duckdb
import pyarrow as pa
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from agentic_lakehouse.api_gateway import Gateway, Warehouse, create_app
from agentic_lakehouse.compiler import SqlCompiler
from agentic_lakehouse.config import Rollup
from agentic_lakehouse.engines import DuckDBEngine
from agentic_lakehouse.lookups import read_lookups
from agentic_lakehouse.ontology import Ontology
from agentic_lakehouse.rollups import materialize
from agentic_lakehouse.views import parse_view
from test_compiler import ONTOLOGY
from test_rollups import FLUSHED, catalog, source  # noqa: F401 - fixtures
from test_router import COMPILER, EVERYTHING, ROWS, query, router

SIZES = {"comcast": "large", "verizon": "large", "o'reilly": "tiny"}  # att isn't listed
REGIONS = [{"isp": "comcast", "region": "east", "subscribers": 30.5}, {"isp": "att", "region": "south", "subscribers": None}]
LOOKUPS = [
    {
        "name": "isp_sizes",
        "description": "How big each ISP is.",
        "key": "isp",
        "rows": [{"isp": isp, "size": size} for isp, size in SIZES.items()],
        "attributes": [{"name": "size", "type": "string", "label": "ISP size", "missing": "unknown"}],
    },
    {
        "name": "isp_regions",
        "description": "Where each ISP is, and its subscribers in millions.",
        "key": "isp",
        "table": "gold.isp_regions",
        "attributes": [{"name": "region", "type": "string"}, {"name": "subscribers", "type": "number"}],
    },
]


def ontology(lookups=LOOKUPS) -> Ontology:
    return Ontology.model_validate(ONTOLOGY.model_dump(exclude_defaults=True) | {"lookups": copy.deepcopy(lookups)})


WITH_LOOKUPS = ontology()
PLAYBACK = next(m for m in WITH_LOOKUPS.models if m.name == "playback")


def run(compiler, q) -> list[dict]:
    return EVERYTHING.execute(compiler.compile(q, engine="duckdb", table="playback")).to_pylist()


def test_a_lookups_attributes_become_dimensions_of_every_model_with_its_key():
    dimensions = {d.name: d for d in PLAYBACK.dimensions}
    assert (dimensions["size"].lookup, dimensions["size"].label, dimensions["region"].lookup) == ("isp_sizes", "ISP size", "isp_regions")
    billing = next(m for m in WITH_LOOKUPS.models if m.name == "billing")  # has no isp
    assert {d.name for d in billing.dimensions} == {"billed_at"}


@pytest.mark.parametrize("engine", ["pinot", "trino", "duckdb"])
def test_an_attribute_compiles_to_a_case_over_its_key_on_every_engine(engine):
    sql = SqlCompiler(WITH_LOOKUPS).compile(query(0, 6, metrics=["plays"], dimensions=["size"]), engine=engine, table="t")
    assert "CASE" in sql and "'large'" in sql and "'o''reilly'" in sql and "'unknown'" in sql


def test_an_attribute_slices_every_aggregation_exactly_across_the_tiers():
    q = query(0, 6, metrics=["plays", "viewers", "avg_latency_ms"], dimensions=["size"])
    result = router(compiler=SqlCompiler(WITH_LOOKUPS)).run(q)
    assert not result.notes
    size = lambda isp: SIZES.get(isp, "unknown")  # noqa: E731
    groups = sorted({size(r[1]) for r in ROWS})
    expected = [
        {
            "size": g,
            "plays": sum(1 for r in ROWS if size(r[1]) == g),
            "viewers": len({r[3] for r in ROWS if size(r[1]) == g}),
            "avg_latency_ms": pytest.approx(sum(r[4] for r in ROWS if size(r[1]) == g) / sum(1 for r in ROWS if size(r[1]) == g)),
        }
        for g in groups
    ]
    assert result.table.to_pylist() == expected


def test_a_table_lookup_is_empty_until_its_rows_are_read():
    assert {r["region"] for r in run(SqlCompiler(WITH_LOOKUPS), query(0, 6, metrics=["plays"], dimensions=["region"]))} == {None}
    loaded = WITH_LOOKUPS.with_lookup_rows({"isp_regions": REGIONS})
    rows = {r["isp"]: r for r in run(SqlCompiler(loaded), query(0, 6, metrics=["plays"], dimensions=["isp", "region", "subscribers"]))}
    assert (rows["comcast"]["region"], rows["comcast"]["subscribers"]) == ("east", 30.5)
    assert (rows["att"]["region"], rows["att"]["subscribers"], rows["verizon"]["region"]) == ("south", None, None)  # no missing: set


def test_read_lookups_reads_each_table_through_an_engine():
    asked = []

    def query_engine(sql: str) -> pa.Table:
        asked.append(sql)
        return pa.Table.from_pylist(REGIONS)

    loaded, errors = read_lookups(WITH_LOOKUPS, query_engine)
    assert (asked, errors) == (["SELECT isp, region, subscribers FROM gold.isp_regions"], [])
    assert {r["isp"]: r["region"] for r in run(SqlCompiler(loaded), query(0, 6, metrics=["plays"], dimensions=["isp", "region"]))}["comcast"] == "east"


def test_a_lookup_that_cant_be_read_keeps_its_rows_and_says_why():
    seeded = WITH_LOOKUPS.with_lookup_rows({"isp_regions": REGIONS})

    def failing(sql: str) -> pa.Table:
        raise RuntimeError("table not found")

    loaded, errors = read_lookups(seeded, failing)
    assert errors == ["lookup isp_regions: couldn't read gold.isp_regions (table not found); keeping its last rows"]
    assert loaded.with_lookup_rows({}).model_dump() == seeded.model_dump()


def test_the_gateway_refreshes_lookups_through_its_warehouse():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA gold; CREATE TABLE gold.isp_regions (isp VARCHAR, region VARCHAR, subscribers DOUBLE)")
    con.execute("INSERT INTO gold.isp_regions VALUES ('verizon', 'north', 12.0)")
    gateway = Gateway(router(compiler=SqlCompiler(WITH_LOOKUPS)), warehouse=Warehouse(DuckDBEngine(con)))
    assert gateway.refresh_lookups() == []
    q = query(0, 6, metrics=["plays"], dimensions=["isp", "region"])
    regions = {r["isp"]: r["region"] for r in gateway.router.run(q).table.to_pylist()}
    assert regions == {"att": None, "comcast": None, "verizon": "north"}


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda l: l[0].update(rows=[{"isp": f"isp{i}", "size": "x"} for i in range(10_001)]), r"isp_sizes has 10001 rows; at most 10000"),
        (lambda l: l[0]["rows"].append({"isp": "comcast", "size": "small"}), r"isp_sizes lists 'comcast' twice"),
        (lambda l: l[0].update(key="nowhere"), r"isp_sizes is keyed by 'nowhere', which no model has as a dimension"),
        (lambda l: l[0]["attributes"].append({"name": "is_mobile", "type": "boolean"}), r"attribute 'is_mobile' of lookup isp_sizes is already a dimension of model playback"),
        (lambda l: l[0]["attributes"].append({"name": "plays", "type": "number"}), r"attribute 'plays' of lookup isp_sizes is a metric's name"),
        (lambda l: l[1].pop("table"), r"isp_regions has neither rows nor a table"),
    ],
)
def test_a_lookup_is_checked_when_the_ontology_loads(change, message):
    lookups = copy.deepcopy(LOOKUPS)
    change(lookups)
    with pytest.raises(ValidationError, match=message):
        ontology(lookups)


def test_an_ontology_without_lookups_compiles_as_before():
    plain = Ontology.model_validate(ONTOLOGY.model_dump(exclude_defaults=True))
    q = query(0, 6, metrics=["plays", "slow_rate"], dimensions=["isp", "is_mobile"])
    assert plain.lookups == [] and SqlCompiler(plain).compile(q, engine="trino", table="t") == COMPILER.compile(q, engine="trino", table="t")


def test_a_reloaded_ontology_doesnt_add_its_attributes_twice():
    again = Ontology.model_validate(WITH_LOOKUPS.model_dump())
    assert [d.name for d in next(m for m in again.models if m.name == "playback").dimensions] == [d.name for d in PLAYBACK.dimensions]


# ---------------------------------------------------------------- views, the catalog, startup, rollups


def test_views_and_from_metrics_slice_by_an_attribute():
    from test_views import TABLES

    header = "/*\ndescription: A test view.\n*/\n"
    for source in ("playback", "metrics"):
        v = parse_view("test", header + f"SELECT size, plays FROM {source} WHERE ts >= now() - INTERVAL '6' HOUR", WITH_LOOKUPS, TABLES)
        assert [(c.name, c.type, c.label) for c in v.columns] == [("size", "string", "ISP size"), ("plays", "number", "Plays")]


def test_the_catalog_says_which_dimensions_come_from_a_lookup():
    [model] = Gateway(router(compiler=SqlCompiler(WITH_LOOKUPS))).catalog().models
    assert {d.name: d.lookup for d in model.dimensions} == {"isp": None, "is_mobile": None, "size": "isp_sizes", "region": "isp_regions", "subscribers": "isp_regions"}


def test_the_gateway_reads_its_lookups_before_it_serves():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA gold; CREATE TABLE gold.isp_regions AS SELECT 'att' AS isp, 'south' AS region, 4.0 AS subscribers")
    gateway = Gateway(router(compiler=SqlCompiler(WITH_LOOKUPS)), warehouse=Warehouse(DuckDBEngine(con)))
    with TestClient(create_app(gateway)) as client:
        body = client.post("/query", json={"metrics": ["plays"], "dimensions": ["isp", "region"], "start": "2026-01-01T00:00:00", "end": "2026-01-01T06:00:00"}).json()
    assert {r["isp"]: r["region"] for r in body["rows"]} == {"att": "south", "comcast": None, "verizon": None}


def _tiered_platform():
    """The playback example, with each ISP's tier in a table lookup and an hourly rollup by tier."""
    from test_rollups import PLATFORM

    spec = PLATFORM.ontology.model_dump(exclude_defaults=True)
    spec["lookups"] = [
        {"name": "isp_tiers", "key": "isp_provider", "table": "gold.isp_tiers", "attributes": [{"name": "tier", "type": "string"}]}
    ]
    ontology = Ontology.model_validate(spec)
    ontology._macros = PLATFORM.ontology._macros
    rollup = Rollup.model_validate(
        {"table": "gold.hourly_tier", "description": "d", "lookback_hours": 6, "backfill_days": 1, "every_minutes": 10,
         "query": {"metrics": ["event_count", "unique_sessions"], "dimensions": ["tier"], "grain": "hour"}}
    )  # fmt: skip
    return replace(PLATFORM, ontology=ontology, storage=PLATFORM.storage.model_copy(update={"rollups": [*PLATFORM.storage.rollups, rollup]})), rollup


TIER = "CASE WHEN client_isp < 'm' THEN 'a to l' ELSE 'm to z' END"  # isp_provider is client_isp


def test_a_rollup_reads_the_table_lookups_it_groups_by(source, catalog):
    platform, rollup = _tiered_platform()
    source.con.execute(f"CREATE SCHEMA gold; CREATE TABLE gold.isp_tiers AS SELECT DISTINCT client_isp AS isp_provider, {TIER} AS tier FROM telemetry")
    run = materialize(rollup, platform, engine=source, source_table="telemetry", catalog=catalog, watermark=FLUSHED, log=lambda m: None)
    assert run.skipped is None and run.rows
    stored = sorted(catalog.load_table(rollup.table).scan().to_arrow().to_pylist(), key=lambda r: (r["event_ts"], r["tier"]))
    expected = source.con.execute(
        f"SELECT date_trunc('hour', event_ts) AS event_ts, {TIER} AS tier, count(*) AS event_count, count(DISTINCT session_id) AS unique_sessions "
        f"FROM telemetry WHERE event_ts >= TIMESTAMP '{run.start}' AND event_ts < TIMESTAMP '{run.end}' GROUP BY ALL ORDER BY 1, 2"
    ).to_arrow_table().to_pylist()
    assert [(r["tier"], r["event_count"], r["unique_sessions"]) for r in stored] == [(r["tier"], r["event_count"], r["unique_sessions"]) for r in expected]


def test_a_rollup_whose_lookup_cant_be_read_is_skipped_rather_than_written_empty(source, catalog):
    platform, rollup = _tiered_platform()  # no gold.isp_tiers
    run = materialize(rollup, platform, engine=source, source_table="telemetry", catalog=catalog, watermark=FLUSHED, log=lambda m: None)
    assert "lookup isp_tiers: couldn't read gold.isp_tiers" in run.skipped and "skipped rather than written with empty attributes" in run.skipped
