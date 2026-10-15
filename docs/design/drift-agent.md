# Design: a drift agent that keeps macros and ontology current

Status: designed 2026-10-14; building for v0.6.0.
Tracking: the checklist at the end; update it as work lands.

## The problem

The drift agent exists to keep a project's business definitions, its
ontology and its macros, in step with the data producers send. In v0.5.0 it
does part of that:

- **It notices one kind of drift.** It finds fields producers have started
  sending (new Iceberg columns). It misses a field that stops arriving, and a
  new value in a column that definitions enumerate. Type mismatches are only
  logged, for a person.
- **It only adds to the ontology.** Claude proposes new dimensions and
  metrics. It can call the project's macros but can't write or change one, and
  it never changes an existing definition.
- **It can pay for the same question again.** Drift whose patch was deleted
  unapplied, or that never produced a valid proposal, is asked about again
  every hour.
- **It has never seen real drift.** wiki-pulse's ingest builds a fixed set of
  fields, so upstream changes never reach Iceberg.

## Decisions (with the user, 2026-10-14)

- The drift agent's job is to keep both the macros and the ontology current.
  The gaps above are its missing core.
- Design doc first, then build, test, release, and adopt in wiki-pulse, as
  for SQL access and project macros.

## Design

### Drift signals

Each hourly run compares a dataset's recent data with the week before, and
with its config. What it finds are **signals**:

| Signal | Found from | Example |
|---|---|---|
| New column | the Iceberg schema against `ingestion.yaml`, as today | upstream starts sending `is_temporary` |
| Fading column | how often a configured column is set: the last 24 hours against the 7 days before, from Iceberg's per-file metrics | upstream stops sending `bot` |
| New value | values seen in the last 24 hours but never in the 7 days before, in columns that definitions compare with text | `claude_label` gains `vandalism`, which the `judged_bursts` filter doesn't list |

- **The windows end at the newest event**, read from the flush's
  `agentic-lakehouse.newest-event` table property, not at the wall clock. A
  stalled stream or a sleeping laptop then doesn't look as if every column
  stopped.
- **A column is fading** when the week's share predicts at least 50 set rows
  in the last 24 hours and fewer than a quarter of those arrived. With too few
  recent rows to judge, the run notes that and reports no fading.
- **Column health reads no data.** It sums `record_count`, `value_count` and
  `null_value_count` from `table.inspect.files()`, and assigns each file to a
  window by its time column's bounds. A column added later has no metrics in
  older files and counts as unset there. On wiki-pulse's edits table (631k
  rows in 156 files) the metrics read in 0.2 s.
- **New values are watched in enumerated columns only.** These are text
  columns that a definition compares with string literals (`kind = 'new'`,
  `claude_label IN (...)`, `CASE col WHEN ...`). They're found with sqlglot in
  each snippet's DuckDB rendering, so comparisons inside macros count. A
  dimension that is just a column, like `wiki`, shows a new value as a new
  group and needs no change.
  - The run reads only the time column and the watched columns, up to a cap
    per window.
  - A value is a candidate when it has at least 5 recent rows. A targeted scan
    of the earlier window then confirms it's new.
- **Rename hints.** A new column whose first values arrive within two hours of
  a fading column's last ones, with the same type, is reported as possibly
  replacing it.
- **Lineage.** Each signal lists the definitions that read its column, and the
  macro when the read goes through one.
- Type mismatches and names config can't express are logged for a person, as
  today.

Each signal has a key that stays the same across runs, such as
`edits:fading_column:is_bot`.

### What Claude can propose

The structured output grows from "new dimensions and metrics" to:

| Field | What |
|---|---|
| `dimensions`, `metrics` | New definitions, as today. Metrics can set `label`, `format`, `unit` and `better`; dimensions can set `label`. |
| `changes` | Edits to existing dimensions and metrics: `description`, `sql`, and a metric's `filters`. |
| `macros` | New macros, and new versions of the project's own macros. Each has a name, arguments, a description, its SQL for every engine, and an engine's own SQL where that must differ. |
| `unchanged` | The signals it leaves alone, each with the reason. |

Every item names the signals it answers: S1, S2 and so on, as the request
numbers them. Every signal must be answered. The rules:

- **Names never change and nothing is removed.** Views and dashboards use the
  names, and the history still holds the data. When a column stops arriving,
  the descriptions of the definitions that read it say so.
- **Definitions apply to all of history, in both tiers.** A change must also
  be right for rows from before the drift. Two examples:
  - `coalesce(new_name, old_name)` for a renamed field;
  - reading `editor`, which every row has, rather than `is_temporary`, which
    only new rows have.
- **Macros are the project's business definitions.**
  - Claude can't define or change a macro with a built-in's name
    (`percentile`, `quote`, …).
  - A macro change gives every version the macro should have. Any existing
    engine-specific version it leaves out is removed, so an old override can't
    quietly disagree with a new default.
  - New macros go in `default.sql`, and an engine's own version in
    `<engine>.sql`, created if missing.
  - Each macro gets its description comment directly above it.
- `ingestion.yaml` gets every new column mechanically, as today.

### Validation

A proposal is sent back with its errors, up to three attempts as today. To be
accepted:

1. **The patched config loads** (`load_platform`). Every snippet is rendered
   for every engine, and the compile proof runs per model. Views and steps
   load too.
2. **Every dashboard still checks** (`Project.check`), so the proposal breaks
   no view and no dashboard. Validation now copies `views/` and
   `dashboards/` along with the YAML and macros.
3. **These definitions run on DuckDB over the sampled rows:** every new one,
   every changed one, and every one that calls a changed macro.
4. **Every signal is answered**, and no item names a signal the request
   didn't have.

### What the reviewer sees

The patch header, before the diff, has four parts:

- **The signals**, numbered, each with what answers it.
- **The effect on sampled rows.**
  - It covers each new or changed definition, and each one that calls a
    changed macro.
  - It shows before and after, for the last 24 hours and for the 7 days
    before: a dimension's top values with their shares, and a metric's value.
  - A definition whose value changes on the earlier rows is flagged "rewrites
    history". Sometimes that's the point; the reviewer decides.
- **Engine agreement.** This applies where a changed definition uses
  engine-specific macro versions.
  - sqlglot translates the Trino and Pinot renderings to DuckDB, and they're
    compared with the DuckDB rendering on the sampled rows.
  - Disagreements are listed with examples. Renderings that can't be
    translated are noted.
  - This is a warning, not an error: regex and URL functions differ in edge
    cases.
- **Impact:** the views that use what changed, the dashboards that draw those
  views, and the steps that call a changed macro.

A proposal that changes nothing is still written, as a header with no diff,
so a person sees the reasons. It says there's nothing to apply.

### Spend

- **One question per signal.** `proposals/ledger.json` records each signal
  Claude was asked about: the patch, the time, and the outcome (proposed,
  unchanged, or failed).
  - A signal in the ledger isn't asked about again. That holds while its
    patch is pending, after the patch is applied or deleted, and after a
    failure.
  - Claude is called only for a dataset with a signal that isn't in the
    ledger, and the request carries only those signals.
- `agentic-lakehouse drift --dry-run` prints the signals and what would be
  asked, without calling Claude or writing anything.
- `agentic-lakehouse drift --again` asks again about every current signal.
- Each call logs its tokens: input, cached input, and output.
- **Adaptive thinking is on, at high effort.** Changes that can rewrite
  history are where reasoning pays. max_tokens stays 16,000, without
  streaming. If proposals hit that limit, switch to streaming with a larger
  budget.

### The request

The system prompt states the job and the rules above. The job is to keep the
ontology and the project's macros in step with the data. The request carries:

- the signals, numbered, with their profiles, shares, values, lineage and
  rename hints;
- the dataset's entry in `ingestion.yaml`, whose comments explain the
  columns;
- `ontology.yaml`, and the project's macro files, in full;
- the built-in macro names, which it can't use.

### Code

`agent_sync.py` stays the entry point and keeps its public names. The rest
moves to a package:

| Module | Holds |
|---|---|
| `drift/signals.py` | `ColumnProfile`, `Signal`, `DriftReport`, `DriftSamples`; `detect_drift`; column health, enumerated columns, rename hints, lineage |
| `drift/proposal.py` | the structured output: `Proposal` and its parts |
| `drift/patch.py` | `add_ingestion_columns`; `apply_ontology` (adds and changes, keeping comments); `apply_macros`; the config diff, including new files |
| `drift/review.py` | the header's evidence, engine agreement and impact |
| `agent_sync.py` | the request, the Claude call and retries, validation, the ledger, `render_patch`, `check_drift` |

### wiki-pulse

- **Unknown fields pass through.** `from_recentchange` maps the fields it
  uses, drops the ones it leaves out on purpose, and forwards any other
  top-level field. A field Wikimedia adds then reaches Iceberg and the drift
  agent.
- **Missing fields become nulls, not defaults.** For example, `is_bot` becomes
  `raw.get("bot")` instead of `bool(...)`. A field Wikimedia stops sending then
  shows up as fading, instead of as a column of `false`.
- **The live test.** The transform starts sending `is_temporary`: whether the
  editor's name starts with `~`, as Wikipedia's temporary accounts do.
  Today `is_anonymous` includes them. The agent should propose how
  `editor_type()` and the anonymous metrics treat them. That is one Opus
  proposal, made only after the user approves the spend.

## Implementation plan

| | Workstream | Owner | Files |
|---|---|---|---|
| WS0 | The `drift/` package: move `ColumnProfile`, `DriftReport`, `detect_drift` and the patchers into it; add `Signal`, `DriftSamples` and the new `Proposal`. Today's behavior and tests keep passing, on the new schema. | lead, on `main`, first | `drift/*.py`, `agent_sync.py`, existing tests |
| WS1 | Signals: windows ending at the newest event, column health from file metrics, enumerated columns and new values, rename hints, lineage, and samples that hold rows for each signal | subagent, worktree | `drift/signals.py`, `tests/test_drift_signals.py` |
| WS2 | Patchers: ontology changes that keep comments and styles; labels and formats on new definitions; macro adds and replacements, engine versions, new files, reserved names; diffs that create files | subagent, worktree | `drift/patch.py`, `tests/test_drift_patch.py` |
| WS3 | The system prompt and request; the answered-signals check; validation with views and dashboards; evidence, engine agreement and impact; the ledger; `--dry-run` and `--again`; token logs; adaptive thinking | lead | `agent_sync.py`, `drift/review.py`, `cli.py`, `pipelines/drift.py`, tests |
| WS4 | Merge; an end-to-end test (a macro change on the playback example goes through `check_drift`, applies with `git apply`, and loads); docs; v0.6.0; the wiki-pulse changes; a live dry run; the live proposal | lead | various |

Order: WS0 lands first. WS1, WS2 and WS3 then run in parallel against its
API, and WS4 merges them.

## Testing

- **Signals:**
  - fading from file metrics, including a column added after older files
    were written;
  - no signals when the recent window has too few rows;
  - new values only in enumerated columns, including one compared inside a
    macro;
  - a candidate value that a targeted scan finds in the earlier window is not
    reported;
  - rename hints;
  - windows ending at the newest event, not the wall clock.
- **Patchers:**
  - a changed description, `sql` and filters, with comments and flow style
    kept;
  - a macro replaced together with its description;
  - an engine version added, and one removed, including in a new file;
  - built-in names refused;
  - every patch applies with `git apply` and the result loads.
- **Agent:**
  - an unanswered or unknown signal goes back to Claude;
  - a change that breaks a view or a dashboard goes back to Claude;
  - the header shows before and after, the rewrites-history flag, engine
    disagreements and impact;
  - the ledger stops repeat questions, records failures, and `--again`
    overrides it;
  - `--dry-run` calls nothing.
- **End to end:** the playback example, with a canned Claude reply that
  changes a macro.
- **Live:** a wiki-pulse dry run, then one real proposal.

## Open questions and risks

- **A changed definition can be wrong in ways validation can't see.** The
  effect on sampled rows and the rewrites-history flag are there for the
  reviewer, and nothing is applied without one.
- **The thresholds are guesses:** 24 hours, 7 days, 50 expected rows, a
  quarter, 5 rows. They're keyword arguments for now, and config if a project
  needs other values.
- **Pending patches can conflict**, for example two that both append to one
  model. Apply them in order. A patch that no longer applies can be deleted,
  and `--again` asks afresh.
- **Rows from before a column existed** hold nulls in Iceberg, and may hold
  Pinot's default value instead. Definitions over a new column should treat
  both alike.
- **Big tables:** watched columns are read up to a cap per window, and
  candidate values are confirmed with targeted scans.

## What implementation found

- **Scans of one table don't always share a schema.** A column some files
  predate comes back as `string` from one scan and `large_string` from
  another, so samples are concatenated with permissive type promotion.
- **Samples hold each row once.** A signal's rows are often in its window's
  sample already. Repeating them would skew the before and after numbers.
- **Rename hints treat all number types as one.** The flush makes every new
  numeric field a double, so a renamed int column arrives as a double.
- **A value every comparing definition names already isn't new.** If
  `is_mobile_device` lists `'ios'`, the first iOS events need no change.
- **A column with more than 200 different recent values isn't watched.** For
  example, an identifier compared with one literal would otherwise raise up
  to 20 new-value signals with fresh keys every run.
- **On a wiki-pulse-sized table** (600k rows in 150 files), detection takes
  about 0.5 s: 0.16 s of file metrics, 0.15 s of value reads, 0.23 s of
  samples.

## Checklist

- [x] WS0: the `drift/` package, `Signal`, `DriftSamples`, the new `Proposal`; existing tests pass
- [x] WS1: signals, lineage, samples, tests
- [ ] WS2: patchers, tests
- [ ] WS3: request, validation, review header, ledger, CLI, tests
- [ ] WS4: merge, end-to-end, docs, v0.6.0 release
- [ ] WS4: wiki-pulse changes, live dry run, live proposal (after the user approves the spend)
