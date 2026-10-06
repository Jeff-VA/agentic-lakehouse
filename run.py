"""Command-line entry point for the Modular Agentic Lakehouse Engine.

    python run.py setup                 create topics and tables from config/
    python run.py seed --hours 24       load synthetic telemetry into every tier
    python run.py reset --yes           delete all topics, tables and data, then run setup

Start the containers first: docker compose -f docker/docker-compose.yml up -d
"""

import argparse
from datetime import UTC, datetime, timedelta

from core.config import load_platform
from core.events import generate_events
from core.orchestrator import load_events, setup, teardown


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", default="config", help="config directory (default: ./config)")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("setup", help="create Kafka topics, Pinot tables and Iceberg tables")
    seed = commands.add_parser("seed", help="generate synthetic telemetry and load it into Pinot and Iceberg")
    seed.add_argument("--hours", type=float, default=24, help="length of the window ending at the current hour")
    seed.add_argument("--sessions-per-minute", type=float, default=6)
    seed.add_argument("--seed", type=int, default=0, help="random seed; the same seed and window give the same events")
    reset = commands.add_parser("reset", help="delete all topics, tables and data, then recreate them empty")
    reset.add_argument("--yes", action="store_true", help="confirm deleting all data")
    args = parser.parse_args()

    platform = load_platform(args.config)
    if args.command == "setup":
        setup(platform)
    elif args.command == "reset":
        if not args.yes:
            parser.error("reset deletes all data in Kafka, Pinot and Iceberg; pass --yes to confirm")
        teardown(platform)
        setup(platform)
    elif args.command == "seed":
        end = datetime.now(UTC).replace(tzinfo=None, minute=0, second=0, microsecond=0)
        start = end - timedelta(hours=args.hours)
        dataset = platform.dataset("telemetry")
        events = generate_events(start, end, sessions_per_minute=args.sessions_per_minute, seed=args.seed)
        print(f"generated {len(events)} events from {start:%Y-%m-%d %H:%M} to {end:%Y-%m-%d %H:%M} UTC")
        load_events(platform, dataset, events, through=end)


if __name__ == "__main__":
    main()
