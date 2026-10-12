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
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa

from agentic_lakehouse.compiler import MetricQuery, QueryError, SqlCompiler, bucket_start
from agentic_lakehouse.config import Platform
from agentic_lakehouse.engines import PinotEngine, QueryEngine, TrinoEngine, plain_types
from agentic_lakehouse.orchestrator import flushed_through


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


@dataclass(frozen=True)
class QueryResult:
    table: pa.Table
    start: datetime  # the time range the result covers (UTC), which can be narrower than requested
    end: datetime
    sql: dict[str, str]  # "realtime", "historical" and/or "merge" -> SQL that ran
    notes: list[str] = field(default_factory=list)


class Router:
    def __init__(self, compiler: SqlCompiler, placements: dict[str, Placement], clock: Callable[[], datetime] | None = None):
        self.compiler = compiler
        self.placements = placements  # by dataset name, i.e. a model's `source`
        self.clock = clock or (lambda: datetime.now(UTC))

    @classmethod
    def from_platform(cls, platform: Platform) -> "Router":
        trino = TrinoEngine(platform.deployment.trino)
        pinot = PinotEngine(platform.deployment.pinot.broker_url)
        placements = {
            dataset.name: Placement(
                realtime=Tier(pinot, platform.table_for(dataset.name, "pinot")),
                historical=Tier(trino, platform.table_for(dataset.name, "trino")),
                retention=timedelta(days=dataset.realtime.retention_days),
                flushed_through=lambda name=dataset.name: flushed_through(platform, name),
            )
            for dataset in platform.ingestion.datasets
        }
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
            return self._single(query, placement.historical, "historical")
        if flushed is None or flushed < realtime_from:
            return self._with_gap(query, placement, realtime_from, flushed)

        if query.grain and (boundary := bucket_start(flushed, query.grain)) >= realtime_from:
            return self._split(query, placement, boundary, partial=False)
        if not (unmergeable := self.compiler.unmergeable_metrics(query)):
            return self._split(query, placement, flushed, partial=True)
        result = self._single(query.model_copy(update={"end": flushed}), placement.historical, "historical")
        result.notes.append(
            f"{', '.join(unmergeable)} can't be combined across the real-time and historical tiers, so the "
            f"answer covers historical data up to {flushed:%Y-%m-%d %H:%M} UTC. Add a time grain or start "
            f"after {realtime_from:%Y-%m-%d %H:%M} UTC to include the latest data."
        )
        return result

    def _single(self, query: MetricQuery, tier: Tier, label: str) -> QueryResult:
        sql = self.compiler.compile(query, engine=tier.engine.name, table=tier.table)
        return QueryResult(tier.engine.execute(sql), query.start, query.end, {label: sql})

    def _split(self, query: MetricQuery, placement: Placement, boundary: datetime, *, partial: bool) -> QueryResult:
        parts = {
            "historical": (placement.historical, query.model_copy(update={"end": boundary})),
            "realtime": (placement.realtime, query.model_copy(update={"start": boundary})),
        }
        con = duckdb.connect()
        sql = {}
        for label, (tier, part) in parts.items():
            sql[label] = self.compiler.compile(
                part.model_copy(update={"order_by": [], "limit": None}),
                engine=tier.engine.name,
                table=tier.table,
                partial=partial,
            )
            con.register(label, tier.engine.execute(sql[label]))
        relation = "(SELECT * FROM historical UNION ALL BY NAME SELECT * FROM realtime)"
        sql["merge"] = self.compiler.merge_sql(query, relation, partial=partial)
        return QueryResult(plain_types(con.execute(sql["merge"]).to_arrow_table()), query.start, query.end, sql)

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
            result = self._single(query.model_copy(update={"end": historical_end}), placement.historical, "historical")
            missing = (historical_end, end)
        result.notes.append(
            f"no tier holds events from {missing[0]:%Y-%m-%d %H:%M} to {missing[1]:%Y-%m-%d %H:%M} UTC; "
            "the answer covers the rest of the requested range."
        )
        return result


def _utc(ts: datetime) -> datetime:
    return ts.astimezone(UTC) if ts.tzinfo else ts.replace(tzinfo=UTC)
