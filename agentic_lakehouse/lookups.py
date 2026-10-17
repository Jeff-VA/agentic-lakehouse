"""Lookups' rows from their tables (ontology.Lookup with a `table`).

A lookup whose rows live in a table, such as a pipeline step's output, is read through an engine:
the gateway reads its lookups when it starts and every `refresh_minutes` (Gateway.refresh_lookups),
and a rollup reads those it groups by before each run. Each read compiles the rows into the
attributes' SQL (Ontology.with_lookup_rows). A table that can't be read, or whose rows don't pass
the lookup's checks, leaves the lookup with its last rows, and the error says so.
"""

from collections.abc import Callable

import pyarrow as pa
from pydantic import ValidationError

from agentic_lakehouse.ontology import Ontology


def read_lookups(ontology: Ontology, query: Callable[[str], pa.Table], names: set[str] | None = None) -> tuple[Ontology, list[str]]:
    """`ontology` with its table lookups' rows read again through `query` (SQL over
    <namespace>.<table>), all of them or those in `names`; and what couldn't be read."""
    errors = []
    for lookup in ontology.lookups:
        if lookup.table is None or (names is not None and lookup.name not in names):
            continue
        columns = [lookup.key, *(a.name for a in lookup.attributes)]
        try:
            rows = query(f"SELECT {', '.join(columns)} FROM {lookup.table}").to_pylist()
        except Exception as e:  # noqa: BLE001 - any engine's failure: the lookup keeps its rows
            errors.append(f"lookup {lookup.name}: couldn't read {lookup.table} ({e}); keeping its last rows")
            continue
        try:
            ontology = ontology.with_lookup_rows({lookup.name: rows})
        except ValidationError as e:
            reason = e.errors()[0]["msg"].removeprefix("Value error, ")
            errors.append(f"lookup {lookup.name}: {lookup.table} {reason}; keeping its last rows")
    return ontology, errors
