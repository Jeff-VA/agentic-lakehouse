"""Command-line entry point for the Modular Agentic Lakehouse Engine.

    python run.py setup [--seed-hours 24]  create topics and tables from config/ (and, on a first
                                        start, publish that much synthetic history)
    python run.py seed --hours 24       publish synthetic telemetry to Kafka (Pinot ingests it)
    python run.py stream [--new-fields] publish live synthetic telemetry until stopped (Ctrl-C);
                                        --new-fields simulates an upstream release adding two fields
    python run.py flush [--serve]       move new events from Kafka into Iceberg (once, or on schedule)
    python run.py rollups [--serve]     refresh the gold rollup tables, behind their quality gates
    python run.py drift [--serve]       find new event fields; Claude proposes config patches (proposals/)
    python run.py reset --yes           delete all topics, tables and data, then run setup
    python run.py serve                 REST API and MCP (HTTP) on http://127.0.0.1:8000
    python run.py mcp                   MCP over stdio, for Claude Code or Claude Desktop

`docker compose -f docker/docker-compose.yml up -d --build` runs all of this in
containers; these commands are for development against the same stack.
"""

import argparse
import os
import time
from datetime import UTC, datetime, timedelta

from dotenv import load_dotenv

from core.api_gateway import Gateway, create_app, create_mcp_server
from core.config import load_platform
from core.events import LiveSource, add_release_fields, generate_events
from core.orchestrator import EventPublisher, publish_events, setup, teardown, topic_is_empty


def utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def seed_history(platform, *, hours: float, sessions_per_minute: float = 6, seed: int = 0) -> None:
    """Publish `hours` of synthetic telemetry ending now, so a live stream can carry on from here."""
    end = utc_now().replace(second=0, microsecond=0)
    start = end - timedelta(hours=hours)
    events = generate_events(start, end, sessions_per_minute=sessions_per_minute, seed=seed)
    print(f"generated {len(events)} events from {start:%Y-%m-%d %H:%M} to {end:%Y-%m-%d %H:%M} UTC")
    publish_events(platform, platform.dataset("telemetry"), events)


def run_then_serve(flow, name: str, interval: timedelta, config_dir: str) -> None:
    """Run a Prefect flow now, then on a schedule: a fresh install shouldn't wait a whole interval."""
    try:
        flow(config_dir=config_dir)
    except Exception as e:  # the failed run shows in the Prefect UI; the schedule still starts
        print(f"{name}: first run failed: {e}")
    flow.serve(name=name, interval=interval, parameters={"config_dir": config_dir})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", default="config", help="config directory (default: ./config)")
    commands = parser.add_subparsers(dest="command", required=True)
    setup_command = commands.add_parser("setup", help="create Kafka topics, Pinot tables and Iceberg tables")
    setup_command.add_argument(
        "--seed-hours", type=float, default=0, help="if the topic has never had events, publish this many hours of history"
    )
    seed = commands.add_parser("seed", help="generate synthetic telemetry and publish it to Kafka")
    seed.add_argument("--hours", type=float, default=24, help="length of the window ending at the current hour")
    seed.add_argument("--sessions-per-minute", type=float, default=6)
    seed.add_argument("--seed", type=int, default=0, help="random seed; the same seed and window give the same events")
    stream = commands.add_parser("stream", help="publish live synthetic telemetry until stopped")
    stream.add_argument("--sessions-per-minute", type=float, default=6)
    stream.add_argument("--every", type=float, default=5, help="seconds between batches")
    stream.add_argument(
        "--new-fields", action="store_true", help="simulate an upstream release: events also carry cdn_pop and dropped_frames"
    )
    reset = commands.add_parser("reset", help="delete all topics, tables and data, then recreate them empty")
    reset.add_argument("--yes", action="store_true", help="confirm deleting all data")
    flush = commands.add_parser("flush", help="move new events from Kafka into Iceberg (a Prefect flow)")
    flush.add_argument("--serve", action="store_true", help="keep running, flushing on the configured schedule")
    rollups = commands.add_parser("rollups", help="refresh the gold rollup tables, behind their quality gates")
    rollups.add_argument("--serve", action="store_true", help="keep running on the configured schedule")
    drift = commands.add_parser("drift", help="find new event fields and have Claude propose config patches")
    drift.add_argument("--serve", action="store_true", help="keep running, checking every hour")
    serve = commands.add_parser("serve", help="run the REST API, with MCP at /mcp")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    commands.add_parser("mcp", help="run the MCP server over stdio")
    args = parser.parse_args()
    load_dotenv()  # ANTHROPIC_API_KEY for the drift agent, if kept in .env

    platform = load_platform(args.config)
    if args.command == "setup":
        setup(platform)
        dataset = platform.dataset("telemetry")
        if args.seed_hours and topic_is_empty(platform, dataset):
            seed_history(platform, hours=args.seed_hours)
    elif args.command == "reset":
        if not args.yes:
            parser.error("reset deletes all data in Kafka, Pinot and Iceberg; pass --yes to confirm")
        teardown(platform)
        setup(platform)
    elif args.command == "seed":
        seed_history(platform, hours=args.hours, sessions_per_minute=args.sessions_per_minute, seed=args.seed)
    elif args.command == "stream":
        source = LiveSource(utc_now(), sessions_per_minute=args.sessions_per_minute)
        publisher = EventPublisher(platform, platform.dataset("telemetry"))
        published = 0
        print(f"streaming live telemetry to {publisher.topic}, ~{args.sessions_per_minute:g} new sessions a minute; Ctrl-C stops")
        try:
            while True:
                time.sleep(args.every)
                events = source.poll(utc_now())
                if args.new_fields:
                    add_release_fields(events, source.rng)
                publisher.publish(events)
                published += len(events)
        except KeyboardInterrupt:
            publisher.flush()
            print(f"stopped after {published} events")
    elif args.command == "flush":
        os.environ.setdefault("PREFECT_API_URL", platform.deployment.prefect.api_url)
        from pipelines.flush import flush_all  # after PREFECT_API_URL is set

        if args.serve:
            every = min(t.flush_every_minutes for t in platform.storage.tables)
            run_then_serve(flush_all, "flush", timedelta(minutes=every), args.config)
        else:
            flush_all(config_dir=args.config)
    elif args.command == "rollups":
        os.environ.setdefault("PREFECT_API_URL", platform.deployment.prefect.api_url)
        from pipelines.rollups import refresh_rollups  # after PREFECT_API_URL is set

        if args.serve:
            every = min(r.every_minutes for r in platform.storage.rollups)
            run_then_serve(refresh_rollups, "rollups", timedelta(minutes=every), args.config)
        else:
            refresh_rollups(config_dir=args.config)
    elif args.command == "drift":
        os.environ.setdefault("PREFECT_API_URL", platform.deployment.prefect.api_url)
        from pipelines.drift import drift_check  # after PREFECT_API_URL is set

        if args.serve:
            run_then_serve(drift_check, "drift", timedelta(hours=1), args.config)
        else:
            drift_check(config_dir=args.config)
    elif args.command == "serve":
        import uvicorn

        uvicorn.run(create_app(Gateway.from_platform(platform)), host=args.host, port=args.port)
    elif args.command == "mcp":
        create_mcp_server(Gateway.from_platform(platform)).run("stdio")


if __name__ == "__main__":
    main()
