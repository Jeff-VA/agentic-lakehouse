"""The agentic-lakehouse command. Run it from your project's directory: it reads
config/ there (or --config), and imports project code, such as a dataset's
simulator, from there too.

    init PATH                create a new project in PATH: config, Docker stack, CI,
                             tests and agent files, pinned to this engine version
    setup [--seed-hours 24]  create topics and tables from the config (and, on a first
                             start, publish that much synthetic history)
    seed --hours 24          publish synthetic events to Kafka (Pinot ingests them)
    stream                   publish live synthetic events until stopped (Ctrl-C);
                             --add-field name=type simulates an upstream release
    ingest                   read a dataset's live feed (its `source`) into Kafka until stopped
    process                  run a dataset's processor on its input stream until stopped
    flush [--serve]          move new events from Kafka into Iceberg (once, or on schedule)
    rollups [--serve]        refresh the gold rollup tables, behind their quality gates
    steps [--serve]          run the steps in pipelines.yaml that are due, in order
    drift [--serve]          find new event fields; Claude proposes config patches (proposals/)
    reset --yes              delete all topics, tables and data, then run setup
    serve                    REST API, dashboards and MCP (HTTP) on http://127.0.0.1:8000;
                             open pages reload when a view or dashboard file changes
    mcp                      MCP over stdio, for Claude Code or Claude Desktop
    check                    check the config, views and dashboards (exit 1 on problems)
    screenshot NAME          render dashboard NAME in headless Chrome, with live data:
                             writes NAME.png and lists the problems the page shows

`docker compose -f docker/docker-compose.yml up -d --build` runs all of this in
containers; the command is for development against the same stack.
"""

import argparse
from collections.abc import Callable
import logging
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import get_args

from confluent_kafka import Consumer
from dotenv import load_dotenv

from agentic_lakehouse.api_gateway import Gateway, Project, create_app, create_mcp_server
from agentic_lakehouse.browser import BrowserError
from agentic_lakehouse.scaffold import create_project, engine_version
from agentic_lakehouse.config import ColumnType, Dataset, Platform, load_platform
from agentic_lakehouse.simulate import LiveSource, add_fields, generate_events
from agentic_lakehouse.orchestrator import EventPublisher, last_header, publish_events, setup, teardown, topic_is_empty
from agentic_lakehouse.engines import TrinoEngine
from agentic_lakehouse.processors import ProcessorContext, consumer_group, run_processor
from agentic_lakehouse.sources import EVENT_ID_HEADER, ingest
from agentic_lakehouse.views import qualify_tables


def utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def seed_history(platform: Platform, dataset: Dataset, *, hours: float, rate: float = 6, seed: int = 0) -> None:
    """Publish `hours` of synthetic events ending now, so a live stream can carry on from here."""
    end = utc_now().replace(second=0, microsecond=0)
    start = end - timedelta(hours=hours)
    events = generate_events(dataset, start, end, rate=rate, seed=seed)
    print(f"{dataset.name}: generated {len(events)} events from {start:%Y-%m-%d %H:%M} to {end:%Y-%m-%d %H:%M} UTC")
    publish_events(platform, dataset, events)


def pick_dataset(
    parser: argparse.ArgumentParser, platform: Platform, name: str | None, fits: Callable[[Dataset], bool] = lambda d: True
) -> Dataset:
    """The dataset named on the command line, or the only one in ingestion.yaml that `fits` the command."""
    datasets = {d.name: d for d in platform.ingestion.datasets if fits(d)}
    if not datasets:
        parser.error("no dataset in ingestion.yaml fits this command")
    if name is None and len(datasets) == 1:
        return next(iter(datasets.values()))
    if name not in datasets:
        parser.error(f"pick a dataset with --dataset: {', '.join(datasets)}")
    return datasets[name]


def field_spec(text: str) -> tuple[str, str]:
    name, _, column_type = text.partition("=")
    if not name or column_type not in get_args(ColumnType):
        raise argparse.ArgumentTypeError(f"expected name=type with type one of {', '.join(get_args(ColumnType))}")
    return name, column_type


def run_then_serve(flow, name: str, interval: timedelta, config_dir: str) -> None:
    """Run a Prefect flow now, then on a schedule: a fresh install shouldn't wait a whole interval."""
    try:
        flow(config_dir=config_dir)
    except Exception as e:  # the failed run shows in the Prefect UI; the schedule still starts
        print(f"{name}: first run failed: {e}")
    flow.serve(name=name, interval=interval, parameters={"config_dir": config_dir})


def main() -> None:
    sys.path.insert(0, str(Path.cwd()))  # the project's own code, e.g. a simulator: module:function
    parser = argparse.ArgumentParser(prog="agentic-lakehouse", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", default="config", help="config directory (default: ./config)")
    commands = parser.add_subparsers(dest="command", required=True)
    setup_command = commands.add_parser("setup", help="create Kafka topics, Pinot tables and Iceberg tables")
    setup_command.add_argument(
        "--seed-hours", type=float, default=0, help="if the topic has never had events, publish this many hours of history"
    )
    rate_help = "new episodes a minute (an episode is one event, or one session with a simulator)"
    seed = commands.add_parser("seed", help="generate synthetic events and publish them to Kafka")
    seed.add_argument("--dataset", help="which dataset in ingestion.yaml (default: the only one)")
    seed.add_argument("--hours", type=float, default=24, help="length of the window ending now")
    seed.add_argument("--rate", type=float, default=6, help=rate_help)
    seed.add_argument("--seed", type=int, default=0, help="random seed; the same seed and window give the same events")
    stream = commands.add_parser("stream", help="publish live synthetic events until stopped")
    stream.add_argument("--dataset", help="which dataset in ingestion.yaml (default: the only one)")
    stream.add_argument("--rate", type=float, default=6, help=rate_help)
    stream.add_argument("--every", type=float, default=5, help="seconds between batches")
    stream.add_argument(
        "--add-field",
        type=field_spec,
        action="append",
        default=[],
        metavar="NAME=TYPE",
        help="simulate an upstream release: events also carry this field (repeatable)",
    )
    ingest = commands.add_parser("ingest", help="read a dataset's live feed into Kafka until stopped")
    ingest.add_argument("--dataset", help="which dataset with a source (default: the only one)")
    process = commands.add_parser("process", help="run a dataset's processor on its input stream until stopped")
    process.add_argument("--dataset", help="which dataset with a processor (default: the only one)")
    reset = commands.add_parser("reset", help="delete all topics, tables and data, then recreate them empty")
    reset.add_argument("--yes", action="store_true", help="confirm deleting all data")
    flush = commands.add_parser("flush", help="move new events from Kafka into Iceberg (a Prefect flow)")
    flush.add_argument("--serve", action="store_true", help="keep running, flushing on the configured schedule")
    rollups = commands.add_parser("rollups", help="refresh the gold rollup tables, behind their quality gates")
    rollups.add_argument("--serve", action="store_true", help="keep running on the configured schedule")
    steps = commands.add_parser("steps", help="run the steps in pipelines.yaml that are due, in order")
    steps.add_argument("--serve", action="store_true", help="keep running on the steps' schedules")
    drift = commands.add_parser("drift", help="find new event fields and have Claude propose config patches")
    drift.add_argument("--serve", action="store_true", help="keep running, checking every hour")
    serve = commands.add_parser("serve", help="run the REST API, with MCP at /mcp")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    commands.add_parser("mcp", help="run the MCP server over stdio")
    init = commands.add_parser("init", help="create a new project in a folder")
    init.add_argument("path", help="the new project's folder; it must not exist or be empty")
    init.add_argument("--engine", help="requirements.txt line for the engine (default: this version's release)")
    commands.add_parser("check", help="check the config, views and dashboards")
    screenshot = commands.add_parser("screenshot", help="render a dashboard in headless Chrome, with live data")
    screenshot.add_argument("name", help="the dashboard: its file name in dashboards/ without .html")
    screenshot.add_argument("--theme", choices=["light", "dark"], default="light")
    screenshot.add_argument("--width", type=int, default=1280, help="viewport width in CSS pixels (400 for a phone)")
    screenshot.add_argument("--out", help="where to write the PNG (default: NAME.png)")
    args = parser.parse_args()
    load_dotenv()  # ANTHROPIC_API_KEY for the drift agent, if kept in .env

    if args.command == "init":
        sys.exit(init_project(args.path, args.engine))
    if args.command == "check":  # before loading the config: reporting on it is the point
        sys.exit(check(Project(Path(args.config))))
    if args.command == "screenshot":
        sys.exit(take_screenshot(Project(Path(args.config)), args))

    platform = load_platform(args.config)
    if args.command == "setup":
        setup(platform)
        for dataset in platform.ingestion.datasets:
            if not args.seed_hours:
                continue
            if dataset.fed_by:
                print(f"{dataset.name}: not seeding synthetic history; its events come from {dataset.fed_by}")
            elif topic_is_empty(platform, dataset):
                seed_history(platform, dataset, hours=args.seed_hours)
    elif args.command == "reset":
        if not args.yes:
            parser.error("reset deletes all data in Kafka, Pinot and Iceberg; pass --yes to confirm")
        teardown(platform)
        setup(platform)
    elif args.command == "seed":
        dataset = pick_dataset(parser, platform, args.dataset)
        seed_history(platform, dataset, hours=args.hours, rate=args.rate, seed=args.seed)
    elif args.command == "stream":
        dataset = pick_dataset(parser, platform, args.dataset, lambda d: not d.fed_by)
        source = LiveSource(dataset, utc_now(), rate=args.rate)
        publisher = EventPublisher(platform, dataset)
        extra = dict(args.add_field)
        published = 0
        print(f"streaming {dataset.name} to {publisher.topic}, ~{args.rate:g} new episodes a minute; Ctrl-C stops")
        try:
            while True:
                time.sleep(args.every)
                events = source.poll(utc_now())
                add_fields(events, extra, source.rng)
                publisher.publish(events)
                published += len(events)
        except KeyboardInterrupt:
            publisher.flush()
            print(f"stopped after {published} events")
    elif args.command == "ingest":
        dataset = pick_dataset(parser, platform, args.dataset, lambda d: d.source is not None)
        resume = last_header(platform, dataset, EVENT_ID_HEADER)
        print(f"ingesting {dataset.source.url} into {dataset.topic}" + (f", resuming after {resume}" if resume else ""))
        try:
            ingest(dataset, EventPublisher(platform, dataset), resume_after=resume)
        except KeyboardInterrupt:
            print("stopped")
    elif args.command == "process":
        dataset = pick_dataset(parser, platform, args.dataset, lambda d: d.processor is not None)
        trino = TrinoEngine(platform.deployment.trino)
        context = ProcessorContext(
            dataset,
            platform.dataset(dataset.processor.input),
            query=lambda sql: trino.execute(qualify_tables(sql, platform.deployment.trino.catalog)),
        )
        consumer = Consumer(
            {
                "bootstrap.servers": platform.deployment.kafka.bootstrap_servers,
                "group.id": consumer_group(dataset),
                "enable.auto.commit": False,
            }
        )
        try:
            run_processor(platform, dataset, consumer=consumer, publisher=EventPublisher(platform, dataset), context=context)
        except KeyboardInterrupt:
            print("stopped")
        finally:
            consumer.close()
    elif args.command == "flush":
        os.environ.setdefault("PREFECT_API_URL", platform.deployment.prefect.api_url)
        from agentic_lakehouse.pipelines.flush import flush_all  # after PREFECT_API_URL is set

        if args.serve:
            every = min(t.flush_every_minutes for t in platform.storage.tables)
            run_then_serve(flush_all, "flush", timedelta(minutes=every), args.config)
        else:
            flush_all(config_dir=args.config)
    elif args.command == "rollups":
        os.environ.setdefault("PREFECT_API_URL", platform.deployment.prefect.api_url)
        from agentic_lakehouse.pipelines.rollups import refresh_rollups  # after PREFECT_API_URL is set

        if args.serve:
            every = min(r.every_minutes for r in platform.storage.rollups)
            run_then_serve(refresh_rollups, "rollups", timedelta(minutes=every), args.config)
        else:
            refresh_rollups(config_dir=args.config)
    elif args.command == "steps":
        if not platform.pipelines.steps:
            print("no steps in pipelines.yaml; nothing to run")
            return
        os.environ.setdefault("PREFECT_API_URL", platform.deployment.prefect.api_url)
        from agentic_lakehouse.pipelines.steps import run_steps  # after PREFECT_API_URL is set

        if args.serve:
            every = min(s.every_minutes for s in platform.pipelines.steps)
            run_then_serve(run_steps, "steps", timedelta(minutes=every), args.config)
        else:
            run_steps(config_dir=args.config)
    elif args.command == "drift":
        os.environ.setdefault("PREFECT_API_URL", platform.deployment.prefect.api_url)
        from agentic_lakehouse.pipelines.drift import drift_check  # after PREFECT_API_URL is set

        if args.serve:
            run_then_serve(drift_check, "drift", timedelta(hours=1), args.config)
        else:
            drift_check(config_dir=args.config)
    elif args.command == "serve":
        import uvicorn

        project = Project(platform.config_dir)
        app = create_app(Gateway.from_platform(platform), dashboards_dir=project.dashboards_dir, project=project)
        uvicorn.run(app, host=args.host, port=args.port)
    elif args.command == "mcp":
        create_mcp_server(Gateway.from_platform(platform), project=Project(platform.config_dir)).run("stdio")


def init_project(path: str, engine: str | None) -> int:
    try:
        files = create_project(path, engine=engine)
    except (ValueError, FileExistsError) as e:
        print(e)
        return 1
    print(f"Created {path}: {len(files)} files, on agentic-lakehouse {engine_version()}. Next:")
    print(f"  cd {path}")
    print("  python3 -m venv .venv && .venv/bin/pip install -r requirements.txt pytest")
    print("  docker compose -f docker/docker-compose.yml up -d --build    # then open http://127.0.0.1:8000")
    print("  .venv/bin/agentic-lakehouse check && .venv/bin/pytest")
    return 0


def check(project: Project) -> int:
    """Print what's wrong with the project; 0 if nothing."""
    result = project.check()
    if result.config_error:
        print(f"{project.config_dir}: {result.config_error}")
        return 1
    for dashboard in result.dashboards:
        for problem in dashboard.problems:
            print(problem)
    problems = sum(len(d.problems) for d in result.dashboards)
    counts = [(len(result.views), "view"), (len(result.dashboards), "dashboard"), (problems, "problem")]
    print(f"{project.config_dir}: " + ", ".join(f"{n} {word}{'' if n == 1 else 's'}" for n, word in counts))
    return 1 if problems else 0


def take_screenshot(project: Project, args: argparse.Namespace) -> int:
    for noisy in ("httpx", "mcp"):  # each request the page makes would be logged
        logging.getLogger(noisy).setLevel(logging.WARNING)
    out = Path(args.out or f"{args.name}.png")
    try:
        shot = project.screenshot(args.name, theme=args.theme, width=args.width)
    except KeyError:
        print(f"no dashboard named '{args.name}' in {project.dashboards_dir}")
        return 1
    except (BrowserError, ValueError) as e:  # ConfigError is a ValueError
        print(e)
        return 1
    out.write_bytes(shot.png)
    print(f"wrote {out}" + ("" if shot.settled else " (captured at the time limit: some tags were still loading)"))
    for problem in shot.problems:
        print(f"problem: {problem}")
    for note in shot.notes:
        print(f"note: {note}")
    return 1 if shot.problems or not shot.settled else 0


if __name__ == "__main__":
    main()
