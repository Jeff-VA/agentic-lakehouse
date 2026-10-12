"""agentic-lakehouse init: a new project in its own folder, ready to run.

The project is a working starter (project_template/): a dataset of events, a
model with a few metrics, views, a rollup with quality checks and a
dashboard, plus its own Docker stack, CI, tests and agent files. Its
requirements.txt pins this engine version's release, so the project upgrades
the engine on its own schedule.
"""

import base64
import importlib.metadata
import re
import uuid
from pathlib import Path

RELEASES = "https://github.com/Jeff-VA/agentic-lakehouse/releases/download"
TEMPLATE = Path(__file__).parent / "project_template"
# Packaged without their leading dot, which packaging tools would skip.
DOTTED = {"gitignore": ".gitignore", "dockerignore": ".dockerignore", "mcp.json": ".mcp.json", "github": ".github"}
PROJECT_NAME = re.compile(r"^[a-z][a-z0-9-]*$")


def engine_version() -> str:
    return importlib.metadata.version("agentic-lakehouse")


def release_requirement(version: str) -> str:
    """The requirement line for the engine's release wheel on GitHub."""
    return f"agentic-lakehouse @ {RELEASES}/v{version}/agentic_lakehouse-{version}-py3-none-any.whl"


def create_project(path: str | Path, *, engine: str | None = None) -> list[Path]:
    """Write a new project into `path`, which must not exist or be empty. `engine` is its
    requirements.txt line for the engine (default: this version's release). Returns the files."""
    path = Path(path)
    name = path.resolve().name.lower()
    if not PROJECT_NAME.match(name):
        raise ValueError(f"name the project's folder in lowercase letters, digits and hyphens, like my-project, not {path.name!r}")
    if path.exists() and any(path.iterdir()):
        raise FileExistsError(f"{path} isn't empty")
    version = engine_version()
    replacements = {
        "@@NAME@@": name,
        "@@TITLE@@": name.replace("-", " ").capitalize(),
        "@@VERSION@@": f"v{version}",
        "@@ENGINE@@": engine or release_requirement(version),
        "@@CLUSTER_ID@@": base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b"=").decode(),  # Kafka's own format
    }
    written = []
    for source in sorted(TEMPLATE.rglob("*")):
        if source.is_dir() or "__pycache__" in source.parts:
            continue
        target = path.joinpath(*(DOTTED.get(part, part) for part in source.relative_to(TEMPLATE).parts))
        text = source.read_text()
        for placeholder, value in replacements.items():
            text = text.replace(placeholder, value)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
        written.append(target)
    (path / "proposals").mkdir(exist_ok=True)  # the drift agent's patches, mounted into its container
    return written
