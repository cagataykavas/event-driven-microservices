from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from event_platform.simulation import run_reference_scenario
from event_platform.storage import Database


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a transactional event-processing scenario")
    parser.add_argument("--database", default=":memory:")
    parser.add_argument("--duplicate-deliveries", type=int, default=2)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    with Database(args.database) as database:
        report = run_reference_scenario(database, duplicate_deliveries=args.duplicate_deliveries)
    rendered = json.dumps(asdict(report), indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
    print(rendered, end="")


if __name__ == "__main__":
    main()
