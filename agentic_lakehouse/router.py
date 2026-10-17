"""Send each semantic query to the tier(s) holding its time range.

The real-time tier (Pinot) keeps the last `retention_days` of events; the
historical tier (Iceberg, through Trino) holds everything up to its flush
watermark. For a query over [start, end):

1. start inside Pinot's retention: Pinot answers alone (freshest data).
2. end at or before the watermark: Trino answers alone.
3. Otherwise both tiers are needed:
   a. With a time grain whose bucket boundary at the watermark falls inside
      Pinot's retention, split there. Each bucket comes from one tier, so
      every metric is exact; DuckDB orders and limits the union.
   b. Else, if every metric can be combined (counts, sums, min/max,
      averages, ratios), each tier returns partial aggregates split at the
      watermark and DuckDB merges them.
   c. Else, with no time grain, distinct counts and percentiles are merged
      using the real-time tier's values: it covers only the minutes since the
      flush, so it lists them. Distinct counts merge exactly (Trino counts how
      many of those values it saw too); percentiles approximately (Trino gives
      its distribution as a grid of quantiles, read off with the values).
   d. Else (a week or month grain, or more real-time values than
      max_realtime_values): Trino answers up to the watermark, and the result
      says the most recent data isn't covered.

Every result reports the time range it covers, the SQL each tier ran and any
caveats, so clients (including AI agents) can tell what they got.

The historical tier only changes when the flush commits, so its results are
kept, by their SQL and the Iceberg snapshot they read, and reused until the
next commit (docs/design/split-query-cache.md). A live view refreshes every
few seconds; a rolling window ("the last 30 days") starts a little later each
time, so in a split the historical half is cut at the first boundary after its
start: the edge before it runs each time, and the body after it is reused.
"""

import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa

from agentic_lakehouse.compiler import Grain, MetricQuery, QueryError, SqlCompiler, bucket_start, merge_quantile
from agentic_lakehouse.config import Platform
from agentic_lakehouse.engines import PinotEngine, QueryEngine, TrinoEngine, plain_types
from agentic_lakehouse.orchestrator import FLUSHED_THROUGH, iceberg_catalog


@dataclass(frozen=True)
class Tier:
    engine: QueryEngine  # its `name` selects the SQL dialect
    table: str


@dataclass(frozen=True)
class Placement:
    """Where one dataset's events live."""

    realtime: Tier
    historical: Tier
    retention: timedelta  # how far back the real-time tier reaches
    flushed_through: Callable[[], datetime | None]  # historical tier is complete before this; naive means UTC
    # The historical table's current Iceberg snapshot: a result computed from it holds until the next
    # commit, so it can be reused till then. None, the default, reuses nothing.
    snapshot: Callable[[], int | None] = lambda: None


@dataclass(frozen=True)
class QueryResult:
    table: pa.Table
    start: datetime  # the time range the result covers (UTC), which can be narrower than requested
    end: datetime
    sql: dict[str, str]  # "realtime", "historical" and/or "merge" -> SQL that ran
    notes: list[str] = field(default_factory=list)


class ResultCache:
    """Results the historical tier computed, by their SQL and the Iceberg snapshot they read: the
    same SQL over the same snapshot gives the same rows. The results used longest ago go first once
    they hold more than `max_bytes`. Views run in threads, so a lock guards it."""

    def __init__(self, max_bytes: int = 64 * 2**20):
        self.max_bytes = max_bytes
        self._results: OrderedDict[tuple, pa.Table] = OrderedDict()
        self._bytes = 0
        self._lock = threading.Lock()

    def get(self, key: tuple) -> pa.Table | None:
        with self._lock:
            rows = self._results.get(key)
            if rows is not None:
                self._results.move_to_end(key)
            return rows

    def put(self, key: tuple, rows: pa.Table) -> None:
        if rows.nbytes > self.max_bytes:
            return  # it would push out everything else
        with self._lock:
            if key in self._results:
                self._bytes -= self._results.pop(key).nbytes
            self._results[key] = rows
            self._bytes += rows.nbytes
            while self._bytes > self.max_bytes:
                _, evicted = self._results.popitem(last=False)
                self._bytes -= evicted.nbytes


class Router:
    def __init__(
        self,
        compiler: SqlCompiler,
        placements: dict[str, Placement],
        clock: Callable[[], datetime] | None = None,
        cache: ResultCache | None = None,
        max_realtime_values: int = 5000,
    ):
        self.compiler = compiler
        self.placements = placements  # by dataset name, i.e. a model's `source`
        self.clock = clock or (lambda: datetime.now(UTC))
        self.cache = cache if cache is not None else ResultCache()
        # How many values the real-time tier may list, from the minutes since the flush, for distinct
        # counts and percentiles to be merged; past it (a flush long stalled), they're answered from
        # history, as before.
        self.max_realtime_values = max_realtime_values

    @classmethod
    def from_platform(cls, platform: Platform) -> "Router":
        trino = TrinoEngine(platform.deployment.trino)
        pinot = PinotEngine(platform.deployment.pinot.broker_url)
        placements = {}
        for dataset in platform.ingestion.datasets:
            table = _IcebergTable(platform, dataset.name)
            placements[dataset.name] = Placement(
                realtime=Tier(pinot, platform.table_for(dataset.name, "pinot")),
                historical=Tier(trino, platform.table_for(dataset.name, "trino")),
                retention=timedelta(days=dataset.realtime.retention_days),
                flushed_through=table.flushed_through,
                snapshot=table.snapshot,
            )
        return cls(SqlCompiler(platform.ontology), placements)

    def run(self, query: MetricQuery) -> QueryResult:
        model = self.compiler.model_for(query)
        placement = self.placements.get(model.source)
        if placement is None:
            raise QueryError(f"model '{model.name}' reads dataset '{model.source}', which no tier holds")
        start, end = query.start, query.end
        realtime_from = _utc(self.clock()) - placement.retention
        flushed = placement.flushed_through()
        flushed = flushed and _utc(flushed)

        if start >= realtime_from:
            return self._single(query, placement.realtime, "realtime")
        if flushed is not None and end <= flushed:
            return self._single(query, placement.historical, "historical", placement.snapshot())
        if flushed is None or flushed < realtime_from:
            return self._with_gap(query, placement, realtime_from, flushed)

        if query.grain and (boundary := bucket_start(flushed, query.grain)) >= realtime_from:
            return self._split(query, placement, boundary, partial=False)
        if not (unmergeable := self.compiler.unmergeable_metrics(query)):
            return self._split(query, placement, flushed, partial=True)
        why = f"{', '.join(unmergeable)} can't be combined across the real-time and historical tiers"
        if not query.grain and self.compiler.combinable_given_values(query):
            if (result := self._split_by_values(query, placement, flushed)) is not None:
                return result
            why = (
                f"{', '.join(unmergeable)}: the real-time tier saw more than {self.max_realtime_values} distinct "
                "values since the last flush, too many to combine exactly"
            )
        result = self._single(query.model_copy(update={"end": flushed}), placement.historical, "historical", placement.snapshot())
        result.notes.append(
            f"{why}, so the answer covers historical data up to {flushed:%Y-%m-%d %H:%M} UTC. Add a time "
            f"grain or start after {realtime_from:%Y-%m-%d %H:%M} UTC to include the latest data."
        )
        return result

    def _single(self, query: MetricQuery, tier: Tier, label: str, snapshot: int | None = None) -> QueryResult:
        sql = self.compiler.compile(query, engine=tier.engine.name, table=tier.table)
        rows, reused = self._run(sql, tier, snapshot)
        return QueryResult(rows, query.start, query.end, {label: _reused(sql, snapshot) if reused else sql})

    def _run(self, sql: str, tier: Tier, snapshot: int | None) -> tuple[pa.Table, bool]:
        """`sql`'s rows, and whether they were reused: a historical result computed from `snapshot`
        holds until the next commit. Without a snapshot, it just runs."""
        if snapshot is None:
            return tier.engine.execute(sql), False
        key = (tier.engine.name, tier.table, snapshot, sql)
        if (rows := self.cache.get(key)) is not None:
            return rows, True
        rows = tier.engine.execute(sql)
        self.cache.put(key, rows)
        return rows, False

    def _split(self, query: MetricQuery, placement: Placement, boundary: datetime, *, partial: bool) -> QueryResult:
        con = duckdb.connect()
        sql = {}
        historical = query.model_copy(update={"end": boundary, "order_by": [], "limit": None})
        sql["historical"], rows = self._historical_half(historical, placement.historical, placement.snapshot(), partial=partial)
        con.register("historical", rows)
        realtime = query.model_copy(update={"start": boundary, "order_by": [], "limit": None})
        sql["realtime"] = self.compiler.compile(realtime, engine=placement.realtime.engine.name, table=placement.realtime.table, partial=partial)
        con.register("realtime", placement.realtime.engine.execute(sql["realtime"]))
        relation = "(SELECT * FROM historical UNION ALL BY NAME SELECT * FROM realtime)"
        sql["merge"] = self.compiler.merge_sql(query, relation, partial=partial)
        return QueryResult(plain_types(con.execute(sql["merge"]).to_arrow_table()), query.start, query.end, sql)

    def _split_by_values(self, query: MetricQuery, placement: Placement, boundary: datetime) -> QueryResult | None:
        """A split whose distinct counts and percentiles are merged too, using the real-time half's
        values: it covers only the minutes since the flush, so it can list them.
        - Distinct counts, exactly: |A ∪ B| = |A| + |B| − |A ∩ B|. The historical half counts its own
          values (A) and how many of the real-time half's (B) it saw too; the merge subtracts them.
        - Percentiles: the historical half gives its distribution as a grid of quantiles, and the
          merge reads the percentile off it and the real-time values together (merge_quantile).
        None if the real-time half lists more than max_realtime_values. The historical half isn't
        reused from the cache: its SQL names the newest values, which change with every query."""
        realtime_tier, historical_tier = placement.realtime, placement.historical
        realtime = query.model_copy(update={"start": boundary, "order_by": [], "limit": None})
        sql, keys, values = {}, {}, {}
        for label, parts in (("realtime_keys", self.compiler.distinct_parts(query)), ("realtime_values", self.compiler.percentile_parts(query))):
            for part in parts:
                values_sql = self.compiler.values_sql(realtime, part, engine=realtime_tier.engine.name, table=realtime_tier.table)
                sql[label] = f"{sql[label]};\n{values_sql}" if label in sql else values_sql
                rows = realtime_tier.engine.execute(values_sql)
                if label == "realtime_keys":
                    keys[part.name] = [tuple(r[d] for d in query.dimensions) + (r["_value"],) for r in rows.to_pylist() if r["_value"] is not None]
                else:
                    values[part.name] = rows
        if sum(map(len, keys.values())) + sum(t.num_rows for t in values.values()) > self.max_realtime_values:
            return None

        con = duckdb.connect()
        historical = query.model_copy(update={"end": boundary, "order_by": [], "limit": None})
        sql["historical"] = self.compiler.compile(
            historical, engine=historical_tier.engine.name, table=historical_tier.table, partial=True, overlap=keys, grids=True
        )
        con.register("historical", historical_tier.engine.execute(sql["historical"]))
        sql["realtime"] = self.compiler.compile(realtime, engine=realtime_tier.engine.name, table=realtime_tier.table, partial=True, overlap={})
        con.register("realtime", realtime_tier.engine.execute(sql["realtime"]))
        parts = ["SELECT * FROM historical", "SELECT * FROM realtime"]
        for part in self.compiler.percentile_parts(query):  # each group's percentile, from both halves
            con.register(f"values__{part.name}", values[part.name])
            inputs = con.execute(self.compiler.percentile_inputs_sql(query, part, "historical", f'"values__{part.name}"')).to_arrow_table()
            merged = [merge_quantile(**row, fraction=part.percentile) for row in inputs.drop_columns(query.dimensions).to_pylist()]
            con.register(f"merged__{part.name}", inputs.select(query.dimensions).append_column(f"_merged__{part.name}", pa.array(merged, pa.float64())))
            parts.append(f'SELECT * FROM "merged__{part.name}"')
        relation = f"({' UNION ALL BY NAME '.join(parts)})"
        sql["merge"] = self.compiler.merge_sql(query, relation, partial=True)
        return QueryResult(plain_types(con.execute(sql["merge"]).to_arrow_table()), query.start, query.end, sql)

    def _historical_half(self, part: MetricQuery, tier: Tier, snapshot: int | None, *, partial: bool) -> tuple[str, pa.Table]:
        """The SQL and rows of a split's historical half. With a snapshot it's cut at the first
        boundary after its start: the edge before it, which moves with a rolling window, runs each
        time; the body after it is reused until the next commit. A grained split cuts at a bucket
        boundary, so each bucket's rows come from one piece; partial aggregates merge across any
        boundary, so they're cut at the hour."""
        def compile(piece: MetricQuery) -> str:
            return self.compiler.compile(piece, engine=tier.engine.name, table=tier.table, partial=partial)

        cut = boundary_after(part.start, "hour" if partial or not part.grain else part.grain)
        if snapshot is None or part.start < cut >= part.end:  # nothing to reuse, or no body to reuse
            sql = compile(part)
            return sql, tier.engine.execute(sql)
        if cut == part.start:  # an aligned start: no edge
            sql = compile(part)
            rows, reused = self._run(sql, tier, snapshot)
            return (_reused(sql, snapshot) if reused else sql), rows
        edge, body = compile(part.model_copy(update={"end": cut})), compile(part.model_copy(update={"start": cut}))
        body_rows, reused = self._run(body, tier, snapshot)
        rows = pa.concat_tables([tier.engine.execute(edge), body_rows], promote_options="permissive")
        return f"-- runs each time, up to {cut:%Y-%m-%d %H:%M} UTC\n{edge};\n{_reused(body, snapshot) if reused else body}", rows

    def _with_gap(
        self, query: MetricQuery, placement: Placement, realtime_from: datetime, flushed: datetime | None
    ) -> QueryResult:
        # The historical tier stops before the real-time tier begins: the flush is
        # behind, or nothing has been flushed yet. Answer from the tier holding
        # more of the requested range, and say what's missing.
        start, end = query.start, query.end
        historical_end = max(start, min(end, flushed)) if flushed else start
        realtime_start = min(end, max(start, realtime_from))
        if historical_end == start and realtime_start == end:
            reason = (
                "nothing has been flushed to the historical tier yet"
                if flushed is None
                else f"the historical tier ends at {flushed:%Y-%m-%d %H:%M} UTC"
            )
            raise QueryError(
                f"no tier holds events from {start:%Y-%m-%d %H:%M} to {end:%Y-%m-%d %H:%M} UTC: {reason}, "
                f"and the real-time tier starts at {realtime_from:%Y-%m-%d %H:%M} UTC"
            )
        if end - realtime_start >= historical_end - start:
            result = self._single(query.model_copy(update={"start": realtime_start}), placement.realtime, "realtime")
            missing = (start, realtime_start)
        else:
            result = self._single(query.model_copy(update={"end": historical_end}), placement.historical, "historical", placement.snapshot())
            missing = (historical_end, end)
        result.notes.append(
            f"no tier holds events from {missing[0]:%Y-%m-%d %H:%M} to {missing[1]:%Y-%m-%d %H:%M} UTC; "
            "the answer covers the rest of the requested range."
        )
        return result


def boundary_after(ts: datetime, grain: Grain) -> datetime:
    """The first `grain` bucket boundary at or after ts."""
    start = bucket_start(ts, grain)
    if start == ts:
        return ts
    if grain == "month":
        return (start + timedelta(days=32)).replace(day=1)
    return start + {"minute": timedelta(minutes=1), "hour": timedelta(hours=1), "day": timedelta(days=1), "week": timedelta(weeks=1)}[grain]


def _reused(sql: str, snapshot: int | None) -> str:
    return f"-- reused: Iceberg snapshot {snapshot} hasn't changed since this ran\n{sql}"


class _IcebergTable:
    """A dataset's Iceberg table as the router reads it on each query: its watermark and current
    snapshot, both from one load of its metadata, kept for a second."""

    def __init__(self, platform: Platform, dataset: str):
        self.platform, self.dataset = platform, dataset
        self._catalog = None
        self._loaded: tuple[float, object] | None = None
        self._lock = threading.Lock()

    def _table(self):
        with self._lock:
            if self._loaded is None or time.monotonic() - self._loaded[0] > 1:
                self._catalog = self._catalog or iceberg_catalog(self.platform)
                self._loaded = (time.monotonic(), self._catalog.load_table(self.platform.iceberg_table(self.dataset).table))
            return self._loaded[1]

    def flushed_through(self) -> datetime | None:
        value = self._table().properties.get(FLUSHED_THROUGH)
        return datetime.fromisoformat(value) if value else None

    def snapshot(self) -> int | None:
        return self._table().metadata.current_snapshot_id


def _utc(ts: datetime) -> datetime:
    return ts.astimezone(UTC) if ts.tzinfo else ts.replace(tzinfo=UTC)
