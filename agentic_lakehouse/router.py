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
   c. Else (percentiles, distinct counts): Trino answers up to the watermark,
      and the result says the most recent data isn't covered.

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

from agentic_lakehouse.compiler import Grain, MetricQuery, QueryError, SqlCompiler, bucket_start
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
    ):
        self.compiler = compiler
        self.placements = placements  # by dataset name, i.e. a model's `source`
        self.clock = clock or (lambda: datetime.now(UTC))
        self.cache = cache if cache is not None else ResultCache()

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
        result = self._single(query.model_copy(update={"end": flushed}), placement.historical, "historical", placement.snapshot())
        result.notes.append(
            f"{', '.join(unmergeable)} can't be combined across the real-time and historical tiers, so the "
            f"answer covers historical data up to {flushed:%Y-%m-%d %H:%M} UTC. Add a time grain or start "
            f"after {realtime_from:%Y-%m-%d %H:%M} UTC to include the latest data."
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
