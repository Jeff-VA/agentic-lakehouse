"""What Claude can propose about drift. The schema is sent as structured output, so the answer
always parses. Every item names the signals it answers (S1, S2, ..., as the request numbers
them), and every signal gets an answer: a change, or a place in `unchanged` with the reason.

Fields have no defaults on purpose: each is required in the JSON schema, so Claude states null
or [] rather than leaving a field out.
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
