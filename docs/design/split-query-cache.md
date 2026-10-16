# Design: reuse the historical half of split queries

Status: shipped in v0.7.0, 2026-10-16.
Tracking: the checklist at the end.

## The problem

A live view refreshes every 10 seconds (`refresh_seconds`). When its range
reaches back past the real-time tier's retention, the router splits it: Trino
answers up to the flush watermark, Pinot answers after it, and DuckDB merges
the two. Nothing is cached, so every refresh reruns the Trino half. Iceberg
only changes when the flush commits, about every 5 minutes.

Measured on wiki-pulse, about 300k edits a day:

| Query | Time |
|---|---|
| Pinot alone, last 24 hours | 0.06–0.2 s |
| A 3-day split | 0.3–1.3 s |

A 30-day window costs seconds, every 10 seconds, for each view being
watched. Viewers of the same view share one feed, so the cost is per view,
not per viewer.

The idea came from another agent's review: keep the Trino half, keyed on the
Iceberg snapshot ID.

## Why the snapshot alone isn't a key

A rolling window, such as the last 30 days, starts 10 seconds later on each
refresh. Events fall out at the start, so the Trino half is never the same
query twice. Reusing it by snapshot alone would keep counting events that
have left the window. Keying on the SQL and the snapshot is correct, but a
rolling window would then never be reused.

## Design

- **The key is the historical tier's SQL and the Iceberg snapshot it read.**
  The same SQL over the same snapshot gives the same rows. Any commit (a
  flush, late events, compaction) makes a new snapshot, so the next query
  misses.
- **The moving start is split off.** When the router splits a query across
  the tiers, it divides the historical half at the first boundary after the
  start. That covers both kinds of split: a grained split at a bucket
  boundary, and merged partial aggregates.
  - The **edge**, from the start to that boundary, runs every time. It is at
    most one bucket for a grained split, or one hour for merged partials.
  - The **body**, from that boundary to where the historical half ends, is
    reused until the snapshot changes.
  - **Both stay exact.** In a grained split each bucket lies within one
    piece, and the edge holds only the first, partial bucket. Partial
    aggregates merge exactly: counts, sums, minimums, maximums, averages and
    ratios.
- **Queries the historical tier answers alone are reused when they repeat
  exactly.** These are ranges that end before the watermark, unmergeable
  metrics up to the watermark, and the gap case. "Yesterday", or a window
  aligned to the day, repeats exactly. A rolling window of distinct counts or
  percentiles can't be split, so it isn't reused.
- **The router does it**, so live feeds, views, `/sql`, MCP and the parts of
  composed views all benefit.
- **Reading the snapshot costs no extra trip.** The router already loads the
  Iceberg table on every query, for the watermark, and that load also has the
  snapshot. Both come from one load, kept for a second.
- **The cache is bounded and thread-safe.** It's a least-recently-used set of
  Arrow tables, 64 MiB by default. Views run in threads, so a lock guards it.
- **A placement without a snapshot reader runs exactly as today.** That
  includes the stand-in gateway the tests use.
- **The SQL a result reports says what ran.** With `include_sql`, it shows
  which part ran this time and which was reused.

## Expected effect

Each refresh of a split live view costs one small Trino query (the edge), one
Pinot query and a DuckDB merge. The body runs once per flush. With a 5-minute
flush and a 10-second refresh, that's about 30 times less Trino work.

## Testing

- **Reuse:**
  - a repeated split query runs the body once;
  - a moving start reruns only the edge;
  - an aligned window reruns nothing on Trino.
- **Correctness:** results with the cache equal results without it, for
  grained and merged splits, as the start moves.
- **Invalidation:** a new snapshot reruns the body, and late events in the
  body's range appear after their commit.
- **Unchanged:** unmergeable rolling windows and the stand-in run as before.
- **Bound:** the cache evicts the least recently used results past its byte
  limit.
- **Live:** with a 3-day live view open, count Trino's queries in
  `system.runtime.queries`, and time refreshes, before and after.

## Checklist

- [x] Router: the cache, the edge and body split, the snapshot from the same table load
- [x] Tests: 16 in tests/test_router_cache.py; the router's own 15 pass unchanged
- [x] Docs (README, architecture), v0.7.0
- [x] wiki-pulse on v0.7.0, verified live (2026-10-16), with a live `edits_daily` feed (the last
  30 days by day):
  - **Before:** Trino ran its history every 11 seconds, 6 times a minute, about 0.8 s each.
  - **After:** once, on the first refresh. Every refresh since reports "reused: Iceberg snapshot
    … hasn't changed since this ran", until the next flush commits, about every 5 minutes.
  - The daily numbers matched the uncached answers exactly.

## What implementation found

- **Trino's query log is how to see the effect.** `system.runtime.queries` lists every query
  with its text and timing. A query that counts its own kind there matches itself, so leave
  the counting query out of what it counts.
