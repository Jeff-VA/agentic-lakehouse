# Plan: v0.9.1 (NULL ordering) and v0.10.0 (dimension links)

Status: v0.9.1 shipped 2026-10-17; v0.10.0 built and verified live, awaiting review before release.
Tracking: the checklists in each section.

## Where things stand

- **v0.8.0** (released): distinct counts (exact) and percentiles (approximate) merge across
  Pinot and Trino, using the real-time half's values ([architecture](../architecture.md),
  "Which tier answers").
- **v0.9.0** (released): queries across models. Models over one dataset are read together, metrics
  of different datasets are drilled across, the ontology may list metrics across models (ratios),
  and SQL `FROM metrics` reads every model. `FROM <model>` reads its dataset, as before
  (README, "Queries across models").
- **wiki-pulse** runs on v0.9.0, from `~/code/wiki-pulse`, with the Docker stack up.

## How we work

- **Additive only.** What works keeps working: existing tests pass unchanged, unless a test
  asserted the very limitation being lifted. Then it's re-pointed at a case that still holds,
  and that's called out in review.
- **Failing tests first**, shown before implementing, for anything with design in it.
- **Verify live** against wiki-pulse's Pinot and Trino, with the branch engine run from the host,
  before release.
- **Release:** a branch, then fast-forward `main`; bump `pyproject.toml` and the README's pip line;
  commit "Release X.Y.Z: ..."; tag `vX.Y.Z`; push `main` and the tag; wait for `release.yml`.
  Then pin wiki-pulse's `requirements.txt` (both places in the URL), run its tests and
  `agentic-lakehouse check`, commit, push, and rebuild the stack (`docker compose -f
  docker/docker-compose.yml up -d --build`).
- **Commit messages:** a subject line, then why it changed, then
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## v0.9.1: empty values sort last, on every engine

**The bug.** The engine's ORDER BY doesn't say where NULLs go, so each engine uses its own
default. Pinot sorts them first when descending; Trino and DuckDB sort them last. The same
query orders differently depending on the tier that answers. On wiki-pulse, "top wikis by
bytes added, last 24 hours" (Pinot) came back led by tiny wikis that added nothing:
`SELECT wiki, bytes_added FROM edit_content ... ORDER BY bytes_added DESC LIMIT 3`.

**The fix.** Write `NULLS LAST` explicitly, ascending and descending, in every ORDER BY the
engine generates:
- `templates/engine/query.sql` (one engine's query)
- `templates/engine/merge.sql` (a split's merge in DuckDB)
- `SqlCompiler.drill_across_sql` and `SqlCompiler.across_sql`

Trino and DuckDB already sort NULLs last, so only Pinot's answers change. The SQL text changes
everywhere, golden files included (`tests/golden/sample_query.*.sql`).

- [x] Branch `fix/null-ordering`
- [x] Failing tests: every engine's compiled SQL says `NULLS LAST` for ascending and descending
  keys, and so do the merge, drill-across and across-models SQL. On DuckDB, a query ordered by a
  metric that's NULL for some groups puts those groups last, both ways.
- [x] Fix the templates and the two SQL builders; update the golden files
- [x] Check that Pinot's multi-stage engine accepts `NULLS LAST` (live)
- [x] Verify live: the query above leads with the wikis that added the most
- [x] Release v0.9.1, pin wiki-pulse, rebuild, push

## v0.10.0: dimension links (lookup tables)

**The gap.** Models combine on dimensions they share, but nothing can add an attribute of a
dimension's values, such as each wiki's size tier or region. Today that's a hand-written
`CASE` macro, which can't follow data: a wiki's size tier changes as its editing does.

**The design: lookups compiled into the query.** The ontology gains an optional top-level
`lookups:` list. A lookup is keyed by a dimension, such as `wiki`, and adds attributes. Every
model with that dimension gains the attributes as ordinary dimensions:

```yaml
lookups:
  - name: wikis
    description: Each Wikipedia's size tier, by its edits over the last 7 days.
    key: wiki
    table: gold.wiki_sizes      # rows from a table, such as a pipeline step's output
    refresh_minutes: 60         # read again this often
    attributes:
      - {name: size_tier, type: string, label: Size, description: "large, medium or small"}
  - name: regions
    key: language
    rows:                       # or inline, for small fixed mappings
      - {language: en, region: global}
      - {language: de, region: europe}
    attributes:
      - {name: region, type: string}
```

- **How it compiles:** an attribute becomes a dimension whose SQL is
  `CASE <key's SQL> WHEN 'enwiki' THEN 'large' ... END`, built from the lookup's current rows.
  It's an ordinary dimension expression, so it runs on every tier. Every aggregation stays exact,
  distinct counts and percentiles included, with no joins and no Pinot dimension tables. Views,
  drill-across, rollups and the catalog work unchanged, because they read the models' dimensions.
- **Rows:** inline rows compile when the config loads. Table rows are read through the
  historical engine (Trino) when the gateway starts and every `refresh_minutes`; each read swaps
  in a new compiler. Until a table can be read, its attributes are NULL.
- **Semantics: current state (a type 1 slowly changing dimension).** The rows apply to all of
  history: a wiki that grew from medium to large counts as large in last month's data too.
  Validity dates (type 2) are out of scope, and say so in the docs.
- **Limits and checks when the config loads:**
  - At most 10,000 rows, with unique keys.
  - The key must be a dimension of at least one model.
  - An attribute's name can't clash with a dimension of a model that has the key, or with a metric.
  - A key the rows don't list gives a NULL attribute, or the lookup's `missing:` value if it sets one.
- **wiki-pulse:** a step `wiki_sizes` (SQL over `gold.wiki_hourly`, last 7 days) writes each
  wiki's tier: large is 100,000 edits a week or more, medium 10,000 or more, small the rest. The
  lookup `wikis` adds `size_tier`. The first use: revert share and bursts per 10k edits by size tier.

- [x] Branch `feat/dimension-links`
- [x] Failing tests, shown before implementing:
  - inline lookup: the CASE per engine; exact answers split across tiers (counts, a distinct
    count, a percentile); keys and values with quotes; a missing key, with and without `missing:`
  - table lookup: rows read through a stand-in query function; a refresh swaps them; NULL before
    the first read
  - load checks: too many rows, duplicate keys, an unknown key, name clashes
  - additive: an ontology without lookups compiles the same SQL as before
  - views, `FROM metrics` and drill-across can use attributes; the catalog lists them as
    dimensions, saying which lookup they come from
- [x] Implement: ontology (`Lookup`, `LookupAttribute`, `Ontology.lookups`, the compiled
  dimensions), loading and refresh (gateway), catalog
- [x] Verify live on wiki-pulse with the `wiki_sizes` step and the `wikis` lookup
- [x] Docs (README)
- [ ] Show the user, then release v0.10.0, pin wiki-pulse, and add the `wikis` lookup to its ontology

## After these

- **wiki-pulse's own ontology:** `bursts_per_10k_edits` as a metric across models, with a
  minimum-edits filter so tiny wikis don't top rankings.
- **The ML phase:** the classifier's 0.5 threshold, ranking metrics (PR-AUC), label sampling
  bias, and the detector's tail model. Bursts from Oct 16 02:00 to Oct 17 16:20 UTC were judged
  against inflated baselines.
- **Dashboards, then the github.io paper,** which the user writes and Claude helps with.
- **A proposed separate session:** an `event_id` key on wiki-pulse's edits, plus a check for
  duplicate rows.

## What v0.10.0's live check found (2026-10-17)

- `gold.wiki_sizes` (the new wiki-pulse step): 327 wikis, 2 large, 23 medium, 302 small, over the
  5 days of data so far. The lookup reads it in 0.1 s; its CASE has 327 branches and runs on Pinot
  and Trino alike.
- Edits by size tier over 30 hours, split across the tiers (distinct editors included), equal Pinot
  answering alone, for every tier.
- Bursts per 10,000 edits fall with wiki size: large 11.8, medium 6.9, small 3.7; news bursts are
  almost all on the two large wikis (13 of 14).
