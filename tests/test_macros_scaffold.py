"""Project macros where projects start (docs/design/project-macros.md): `init` writes
config/macros/ with an example."""

from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
from sqlglot import exp

from agentic_lakehouse.compiler import SQLGLOT_DIALECTS
from agentic_lakehouse.config import load_platform
from agentic_lakehouse.macros import ENGINES, ProjectMacros
from agentic_lakehouse.ontology import parse_fragment
from agentic_lakehouse.scaffold import create_project
from agentic_lakehouse.schema_compiler import arrow_schema
from agentic_lakehouse.simulate import generate_events


@pytest.fixture(scope="module")
def project(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("projects") / "demo-shop"
    create_project(path)
    return path


def test_init_writes_a_macro_whose_example_works_on_every_engine(project):
    macros = ProjectMacros(project / "config" / "macros")
    [example] = [m for m in macros.catalog() if m.files == ["project/default.sql"]]
    dataset = load_platform(project / "config").dataset("events")
    for engine in ENGINES:
        sql = macros.render(f"{{{{ {example.name}() }}}}", engine)
        columns = {c.name for c in parse_fragment(sql, read=SQLGLOT_DIALECTS[engine]).find_all(exp.Column)}
        assert columns and columns <= {c.name for c in dataset.columns}, engine  # reads the starter dataset

    con = duckdb.connect()
    con.register("events", pa.Table.from_pylist(generate_events(dataset, datetime(2026, 1, 1), datetime(2026, 1, 1, 1)), schema=arrow_schema(dataset)))
    sql = macros.render(f"{{{{ {example.name}() }}}}", "duckdb")
    matching, total = con.execute(f"SELECT count(*) FILTER (WHERE {sql}), count(*) FROM events").fetchone()
    assert 0 < matching < total  # a definition that picks out some of the simulated events


def test_a_new_project_loads_with_its_macros(project):
    assert load_platform(project / "config").ontology.macros.files == ["default"]


@pytest.mark.xfail(
    strict=True,  # passes, and so fails here, once fixed: then remove this mark
    reason="macros._DESCRIBED reads a file's header comment into its first described macro's description",
)
def test_the_examples_comment_describes_it(project):
    [example] = [m for m in ProjectMacros(project / "config" / "macros").catalog() if m.files == ["project/default.sql"]]
    assert example.description == "A large event: an amount of 500 or more."
