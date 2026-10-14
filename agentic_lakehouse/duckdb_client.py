"""DuckDB as a SQL client of the lakehouse: the setup it runs, rendered from the project's config.

`agentic-lakehouse duckdb` writes two files into tools/duckdb/ (and `init` does,
for a new project), from config/deployment.yaml and the gateway's address:

- setup.sql, run once per DuckDB database: the extensions, the S3 login for
  the Iceberg files, and table macros that ask the gateway, which combines live
  data with history: gateway_view(name, params) reads a view, lakehouse_sql(sql)
  runs SQL written like one, and gateway_views() lists the views. Answers come
  as Parquet, so their columns and types survive an empty result, and with
  errors=inline a bad request answers with its error as a row, which DuckDB
  shows, instead of an HTTP status, which it doesn't explain.
- session.sql, run at the start of each session (DBeaver: the connection's
  bootstrap queries, so one statement per line): DuckDB doesn't keep settings
  in the database, nor attached catalogs. It attaches the Iceberg catalog as
  `lake`, for history read directly.

The text depends only on the config and the arguments, so the files can be
committed and rewritten without noise. Design: docs/design/sql-access.md.
"""

import re
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from agentic_lakehouse.config import Platform

GATEWAY_URL = "http://127.0.0.1:8000"  # where the stack, and `agentic-lakehouse serve`, publish the gateway
# The fields of GET /views that gateway_views() reads, typed up front: inferred from the data, an empty
# list (no views, or a view without parameters) has no type, and defaults of mixed types read as JSON.
_CATALOG = 'STRUCT(name VARCHAR, description VARCHAR, source VARCHAR, reads VARCHAR[], params STRUCT(name VARCHAR, "default" VARCHAR)[])[]'


def render(platform: Platform, gateway_url: str = GATEWAY_URL, *, persistent_secret: bool = True) -> dict[str, str]:
    """{"setup.sql": ..., "session.sql": ...} for `platform`, asking the gateway at `gateway_url`.

    `persistent_secret` has DuckDB keep the S3 login for every session, in its folder in the user's
    home (~/.duckdb); without it, the login lasts only for the session that runs setup.sql.
    Raises ValueError if deployment.yaml's s3.endpoint isn't an http(s) URL.
    """
    s3 = platform.deployment.s3
    endpoint = urlsplit(_ipv4(s3.endpoint))
    if endpoint.scheme not in ("http", "https") or not endpoint.netloc:
        raise ValueError(f"s3.endpoint in deployment.yaml should be a URL like http://127.0.0.1:9100, not {s3.endpoint!r}")
    gateway = _ipv4(gateway_url).rstrip("/")
    catalog = _ipv4(platform.deployment.iceberg.catalog_uri)
    project = platform.config_dir.resolve().parent.name if platform.config_dir else "lakehouse"
    kept = "which DuckDB keeps for every session" if persistent_secret else "for this session only"
    download = [
        "-- Download each answer in one request: the gateway runs the query on every request, so reading",
        "-- an answer in ranges, as DuckDB does by default, would run it several times.",
    ]
    setup = [
        f"-- {project} in DuckDB: for DBeaver, the DuckDB CLI or a notebook. Run this file once in a DuckDB",
        "-- database, with the stack running (DuckDB CLI: duckdb my.duckdb < setup.sql; DBeaver: open it in the",
        "-- connection's SQL editor, then Execute SQL Script), and session.sql at the start of each session",
        "-- (DBeaver: the connection's bootstrap queries). It gives two ways in:",
        "--",
        "--   lake.<namespace>.<table>        the Iceberg tables, raw.* and gold.*, read directly: history up",
        "--                                   to the last flush or rollup run (minutes behind). Treat them as",
        "--                                   read-only: the flush, rollups and steps own them.",
        "--",
        f"--   the gateway at {gateway}, which adds the live data to history, so views over a model",
        "--   (and SQL that reads one) are live to the second:",
        "--   gateway_view(name, 'p=v&...')   a view's rows, for these parameters ('' for its defaults)",
        "--   lakehouse_sql('SELECT ...')     the rows of SQL written like a view, without saving it as one",
        "--   gateway_views()                 the views you can ask for: what each answers from, its parameters",
        "--",
        "-- A question the gateway can't answer (an unknown view, a bad parameter or query) comes back as one",
        "-- row, in a column named error. Written by `agentic-lakehouse duckdb` from config/deployment.yaml:",
        "-- run that again, rather than editing this file, when the config changes.",
        "",
        "INSTALL iceberg;",
        "INSTALL httpfs;",
        *download,
        "-- Set here too, because creating gateway_views() below reads the gateway's list of views.",
        "SET force_download = true;",
        "",
        f"-- The S3 login in deployment.yaml, for the files behind lake.*, {kept}",
        f"CREATE OR REPLACE {'PERSISTENT ' if persistent_secret else ''}SECRET {_secret_name(project)} (",
        "    TYPE s3,",
        f"    KEY_ID {_quote(s3.access_key)},",
        f"    SECRET {_quote(s3.secret_key)},",
        f"    ENDPOINT {_quote(endpoint.netloc)},",
        "    URL_STYLE 'path',",
        f"    USE_SSL {str(endpoint.scheme == 'https').lower()},",
        f"    REGION {_quote(s3.region)}",
        ");",
        "",
        "-- A view's rows, e.g. SELECT * FROM gateway_view('<view>', 'hours=24')",
        "CREATE OR REPLACE MACRO gateway_view(name, params) AS TABLE",
        f"    SELECT * FROM read_parquet({_quote(f'{gateway}/views/')} || name || '?format=parquet&errors=inline&' || params);",
        "",
        "-- The rows of SQL written like a view: metrics and dimensions by name from a model, or any SELECT",
        "-- over <namespace>.<table> tables, e.g. SELECT * FROM lakehouse_sql('SELECT ... FROM <model> WHERE ...')",
        "CREATE OR REPLACE MACRO lakehouse_sql(sql) AS TABLE",
        f"    SELECT * FROM read_parquet({_quote(f'{gateway}/sql?format=parquet&errors=inline&q=')} || url_encode(sql));",
        "",
        "-- The views you can ask for: what each answers from, and its parameters with their defaults. A",
        "-- composed view answers live when one of its parts reads a model (tables are <namespace>.<table>).",
        "-- Last, because creating it reads the gateway's list of views.",
        "CREATE OR REPLACE MACRO gateway_views() AS TABLE",
        "    SELECT v.name,",
        "           CASE WHEN v.source = 'model' THEN 'live + history'",
        "                WHEN v.source = 'composed' AND len([r FOR r IN v.reads IF NOT contains(r, '.')]) > 0",
        "                    THEN 'live + history'",
        "                ELSE 'tables (history)' END AS answers_from,",
        "           array_to_string([p.name || '=' || coalesce(p.\"default\", '') FOR p IN v.params], '&') AS params,",
        "           trim(regexp_replace(v.description, '\\s+', ' ', 'g')) AS description",
        "    FROM (SELECT unnest(views) AS v",
        f"          FROM read_json({_quote(f'{gateway}/views')},",
        f"                         columns = {{views: {_quote(_CATALOG)}}}))",
        "    ORDER BY v.name;",
    ]
    session = [
        "-- Run at the start of each DuckDB session, in a database set up with setup.sql (DBeaver: the",
        "-- connection's bootstrap queries, a line each; DuckDB CLI: duckdb -init session.sql my.duckdb).",
        "-- Written by `agentic-lakehouse duckdb` from config/deployment.yaml.",
        *download,
        "SET force_download = true;",
        "-- lake.<namespace>.<table>: the Iceberg tables, through their REST catalog. Treat them as read-only.",
        f"ATTACH IF NOT EXISTS 'warehouse' AS lake (TYPE iceberg, ENDPOINT {_quote(catalog)}, AUTHORIZATION_TYPE 'none');",
    ]
    return {"setup.sql": "\n".join(setup) + "\n", "session.sql": "\n".join(session) + "\n"}


def write(platform: Platform, directory: str | Path, gateway_url: str = GATEWAY_URL) -> list[Path]:
    """Write setup.sql and session.sql into `directory`, creating it if needed. Returns their paths."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    written = []
    for name, text in render(platform, gateway_url).items():
        (directory / name).write_text(text)
        written.append(directory / name)
    return written


def _ipv4(url: str) -> str:
    """`url`, with a localhost host as 127.0.0.1. DuckDB can resolve localhost to IPv6 (::1) first,
    and the Docker VM on a Mac (Colima) forwards the stack's ports over IPv4 only."""
    parts = urlsplit(url)
    if parts.hostname != "localhost":
        return url
    return urlunsplit(parts._replace(netloc="127.0.0.1" + (f":{parts.port}" if parts.port else "")))


def _secret_name(project: str) -> str:
    """The project's S3 secret, named for it: DuckDB keeps persistent secrets per user, not per
    database, so another project's setup mustn't replace this one's."""
    name = re.sub(r"[^a-z0-9]+", "_", project.lower()).strip("_") or "lakehouse"
    if not name[0].isalpha():  # a DuckDB identifier starts with a letter
        name = f"lakehouse_{name}"
    return f"{name}_s3"


def _quote(text: str) -> str:
    """`text` as a SQL string literal."""
    return "'" + text.replace("'", "''") + "'"
