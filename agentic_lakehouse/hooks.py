"""A project's own code, named in its config as "module:attribute".

Simulators, source transforms, processors and pipeline steps are the
project's Python, imported from the folder the agentic-lakehouse command runs
in (or anywhere on the Python path).
"""

import importlib

SPEC = r"^[A-Za-z_][\w.]*:[A-Za-z_]\w*$"  # the form config files use


def load(spec: str):
    """The object `spec` ("package.module:name") names. Raises ImportError saying which part is missing."""
    module, _, name = spec.partition(":")
    try:
        found = importlib.import_module(module)
    except ModuleNotFoundError as e:
        raise ImportError(f"{spec}: can't import {module} ({e}); run from the project's folder") from e
    try:
        return getattr(found, name)
    except AttributeError:
        raise ImportError(f"{spec}: {module} has no {name}") from None
