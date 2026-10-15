"""What Claude can propose about drift. Every item names the signals it answers (S1, S2, ..., as
the request numbers them), and every signal gets an answer: a change, or a place in `unchanged`
with the reason.

The same thing comes in two shapes:

- Proposal and its parts are what the agent works with, with None for "none".
- Answer and its parts are the schema Claude answers in, as structured output, so its answer
  always parses. Structured outputs compile the schema into a grammar, and every nullable field
  (an anyOf with null) makes that grammar exponentially larger: the API allows 16 in a request,
  and refused Proposal's 17 ("the compiled grammar is too large"). So no Answer field is
  nullable. An empty string, "none", "default", "neither" or 0 says "none", and
  Answer.to_proposal() turns those into Nones.

Fields have no defaults on purpose: each is required in the JSON schema, so Claude states every
value rather than leaving one out.
"""

from typing import Literal

from pydantic import BaseModel, Field

SIGNALS = "The signals this answers, by number: S1, S2, ..."


class ProposedDimension(BaseModel):
    """A new dimension."""

    signals: list[str] = Field(description=SIGNALS)
    model: str = Field(description="Name of the ontology model to add it to")
    name: str
    type: Literal["string", "number", "boolean", "timestamp"]
    description: str
    sql: str | None = Field(description="SQL over source columns; null when it is the column of the same name")
    label: str | None = Field(description="How people say it, e.g. 'Editor type'; null for the name in words")


class ProposedMetric(BaseModel):
    """A new metric."""

    signals: list[str] = Field(description=SIGNALS)
    model: str = Field(description="Name of the ontology model to add it to")
    name: str
    description: str
    type: Literal["aggregate", "ratio"]
    agg: Literal["count", "count_distinct", "sum", "avg", "min", "max", "percentile"] | None = Field(
        description="Aggregate metrics only"
    )
    sql: str | None = Field(description="Aggregate metrics: the expression to aggregate; null counts rows")
    percentile: float | None = Field(description="Only with agg percentile, as a fraction, e.g. 0.95")
    filters: list[str] = Field(description="Aggregate metrics: SQL predicates selecting the rows that count")
    numerator: str | None = Field(description="Ratio metrics only: an aggregate metric of the same model")
    denominator: str | None = Field(description="Ratio metrics only: an aggregate metric of the same model")
    label: str | None = Field(description="How people say it, e.g. 'Human edits'; null for the name in words")
    format: Literal["number", "integer", "percent"] | None = Field(
        description="How to show values: percent reads 0.05 as 5%; null for the default (integer for counts)"
    )
    unit: str | None = Field(description="Shown after values, e.g. 'ms'; null for none")
    better: Literal["higher", "lower"] | None = Field(description="Which way a change is good news; null if neither")


class DefinitionChange(BaseModel):
    """A change to an existing dimension or metric. Its name never changes: views and dashboards use it."""

    signals: list[str] = Field(description=SIGNALS)
    model: str
    kind: Literal["dimension", "metric"]
    name: str = Field(description="The existing dimension or metric")
    description: str | None = Field(description="Its new description, or null to keep it")
    sql: str | None = Field(description="Its new SQL, or null to keep it")
    filters: list[str] | None = Field(description="Aggregate metrics: new filters, replacing all of them, or null to keep them")
    reason: str = Field(description="Why, for the reviewer")


class MacroChange(BaseModel):
    """A new macro, or a new version of one of the project's own. It states every version the macro
    should have: an existing engine-specific version that's left out (null) is removed."""

    signals: list[str] = Field(description=SIGNALS)
    name: str
    args: list[str] = Field(description="Argument names; each argument is SQL text, passed in quotes")
    description: str = Field(description="What it means, for people and agents: the comment above the macro")
    sql: str = Field(description="Its SQL for every engine, in config/macros/default.sql")
    pinot: str | None = Field(description="Pinot's own version, only where Pinot needs different SQL; else null")
    trino: str | None = Field(description="Trino's own version, only where Trino needs different SQL; else null")
    duckdb: str | None = Field(description="DuckDB's own version, only where DuckDB needs different SQL; else null")
    reason: str = Field(description="Why, for the reviewer")


class Unchanged(BaseModel):
    """A signal that needs no change, with the reason."""

    signal: str = Field(description="The signal, by number: S1, S2, ...")
    reason: str


class Proposal(BaseModel):
    summary: str = Field(description="A short paragraph for the reviewer: what changed upstream and what you propose")
    dimensions: list[ProposedDimension]
    metrics: list[ProposedMetric]
    changes: list[DefinitionChange]
    macros: list[MacroChange]
    unchanged: list[Unchanged] = Field(description="Every signal you leave alone, with the reason")

    def answers(self) -> dict[str, list[str]]:
        """Each signal number this proposal names, with what answers it, e.g. {"S1": ["metric x"]}."""
        found: dict[str, list[str]] = {}
        items = [(f"dimension {d.name}", d.signals) for d in self.dimensions]
        items += [(f"metric {m.name}", m.signals) for m in self.metrics]
        items += [(f"change to {c.kind} {c.name}", c.signals) for c in self.changes]
        items += [(f"macro {m.name}()", m.signals) for m in self.macros]
        items += [("unchanged", [u.signal]) for u in self.unchanged]
        for label, signals in items:
            for signal in signals:
                found.setdefault(signal, []).append(label)
        return found


# The schema Claude answers in: Proposal's, with nothing nullable (see the module docstring).


class DimensionAnswer(BaseModel):
    signals: list[str] = Field(description=SIGNALS)
    model: str = Field(description="Name of the ontology model to add it to")
    name: str
    type: Literal["string", "number", "boolean", "timestamp"]
    description: str
    sql: str = Field(description="SQL over source columns; empty when it is the column of the same name")
    label: str = Field(description="How people say it, e.g. 'Editor type'; empty for the name in words")


class MetricAnswer(BaseModel):
    signals: list[str] = Field(description=SIGNALS)
    model: str = Field(description="Name of the ontology model to add it to")
    name: str
    description: str
    type: Literal["aggregate", "ratio"]
    agg: Literal["count", "count_distinct", "sum", "avg", "min", "max", "percentile", "none"] = Field(
        description="Aggregate metrics: the aggregation; none for a ratio"
    )
    sql: str = Field(description="Aggregate metrics: the expression to aggregate; empty counts rows, and for a ratio")
    percentile: float = Field(description="Only with agg percentile, as a fraction, e.g. 0.95; otherwise 0")
    filters: list[str] = Field(description="Aggregate metrics: SQL predicates selecting the rows that count")
    numerator: str = Field(description="Ratio metrics only: an aggregate metric of the same model; otherwise empty")
    denominator: str = Field(description="Ratio metrics only: an aggregate metric of the same model; otherwise empty")
    label: str = Field(description="How people say it, e.g. 'Human edits'; empty for the name in words")
    format: Literal["default", "number", "integer", "percent"] = Field(
        description="How to show values: percent reads 0.05 as 5%; default is integer for counts, number otherwise"
    )
    unit: str = Field(description="Shown after values, e.g. 'ms'; empty for none")
    better: Literal["neither", "higher", "lower"] = Field(description="Which way a change is good news")


class ChangeAnswer(BaseModel):
    signals: list[str] = Field(description=SIGNALS)
    model: str
    kind: Literal["dimension", "metric"]
    name: str = Field(description="The existing dimension or metric; names never change")
    description: str = Field(description="Its new description, or empty to keep it")
    sql: str = Field(description="Its new SQL, or empty to keep it")
    replace_filters: bool = Field(description="Aggregate metrics: true to replace all of its filters with `filters`")
    filters: list[str] = Field(description="Its new filters, when replace_filters is true ([] removes them); otherwise []")
    reason: str = Field(description="Why, for the reviewer")


class MacroAnswer(BaseModel):
    signals: list[str] = Field(description=SIGNALS)
    name: str
    args: list[str] = Field(description="Argument names; each argument is SQL text, passed in quotes")
    description: str = Field(description="What it means, for people and agents: the comment above the macro")
    sql: str = Field(description="Its SQL for every engine, in config/macros/default.sql")
    pinot: str = Field(description="Pinot's own version, only where Pinot needs different SQL; otherwise empty")
    trino: str = Field(description="Trino's own version, only where Trino needs different SQL; otherwise empty")
    duckdb: str = Field(description="DuckDB's own version, only where DuckDB needs different SQL; otherwise empty")
    reason: str = Field(description="Why, for the reviewer")


class Answer(BaseModel):
    """Claude's answer, as structured output: a Proposal, with nothing nullable."""

    summary: str = Field(description="A short paragraph for the reviewer: what changed upstream and what you propose")
    dimensions: list[DimensionAnswer]
    metrics: list[MetricAnswer]
    changes: list[ChangeAnswer]
    macros: list[MacroAnswer]
    unchanged: list[Unchanged] = Field(description="Every signal you leave alone, with the reason")

    def to_proposal(self) -> Proposal:
        return Proposal(
            summary=self.summary,
            dimensions=[
                ProposedDimension(
                    signals=d.signals, model=d.model, name=d.name, type=d.type, description=d.description,
                    sql=_none(d.sql), label=_none(d.label),
                )
                for d in self.dimensions
            ],
            metrics=[
                ProposedMetric(
                    signals=m.signals, model=m.model, name=m.name, description=m.description, type=m.type,
                    agg=None if m.agg == "none" else m.agg, sql=_none(m.sql), percentile=m.percentile or None,
                    filters=m.filters, numerator=_none(m.numerator), denominator=_none(m.denominator),
                    label=_none(m.label), format=None if m.format == "default" else m.format, unit=_none(m.unit),
                    better=None if m.better == "neither" else m.better,
                )
                for m in self.metrics
            ],
            changes=[
                DefinitionChange(
                    signals=c.signals, model=c.model, kind=c.kind, name=c.name, description=_none(c.description),
                    sql=_none(c.sql), filters=list(c.filters) if c.replace_filters else None, reason=c.reason,
                )
                for c in self.changes
            ],
            macros=[
                MacroChange(
                    signals=m.signals, name=m.name, args=m.args, description=m.description, sql=m.sql,
                    pinot=_none(m.pinot), trino=_none(m.trino), duckdb=_none(m.duckdb), reason=m.reason,
                )
                for m in self.macros
            ],
            unchanged=list(self.unchanged),
        )  # fmt: skip

    @classmethod
    def from_proposal(cls, proposal: Proposal) -> "Answer":
        """The answer that gives `proposal`: what tests reply with when they stand in for Claude."""
        return cls(
            summary=proposal.summary,
            dimensions=[
                DimensionAnswer(**d.model_dump(exclude={"sql", "label"}), sql=d.sql or "", label=d.label or "")
                for d in proposal.dimensions
            ],
            metrics=[
                MetricAnswer(
                    **m.model_dump(exclude={"agg", "sql", "percentile", "numerator", "denominator", "label", "format", "unit", "better"}),
                    agg=m.agg or "none", sql=m.sql or "", percentile=m.percentile or 0, numerator=m.numerator or "",
                    denominator=m.denominator or "", label=m.label or "", format=m.format or "default", unit=m.unit or "",
                    better=m.better or "neither",
                )
                for m in proposal.metrics
            ],
            changes=[
                ChangeAnswer(
                    **c.model_dump(exclude={"description", "sql", "filters"}), description=c.description or "",
                    sql=c.sql or "", replace_filters=c.filters is not None, filters=c.filters or [],
                )
                for c in proposal.changes
            ],
            macros=[
                MacroAnswer(**m.model_dump(exclude={"pinot", "trino", "duckdb"}), pinot=m.pinot or "", trino=m.trino or "", duckdb=m.duckdb or "")
                for m in proposal.macros
            ],
            unchanged=list(proposal.unchanged),
        )  # fmt: skip


def _none(text: str) -> str | None:
    return text.strip() or None
