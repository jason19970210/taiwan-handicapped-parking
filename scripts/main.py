"""
Main CLI entrypoint for Taiwan Handicapped Parking data management.
Provides commands for data collection and Google Maps synchronization.
"""

import argparse
import logging
import sys
import json
from pathlib import Path

# Add project root to Python path to allow absolute imports
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from scripts.data_collection.merger import DataMerger
from scripts.utils.logger import setup_logger
from scripts.lists.builder import DEFAULT_MAX_ROWS, build_lists
from scripts.lists.maps_sync import DEFAULT_MAX_OPS, DEFAULT_PROFILE, DEFAULT_STATE, sync_lists
from scripts.lists.sync_plan import load_state, make_plan
from scripts.utils.csv_validator import validate_csv_file
from scripts.utils.count_guard import (
    DEFAULT_MAX_DROP_PCT,
    DEFAULT_MAX_GROWTH_PCT,
    check_city_counts,
    city_counts,
    read_committed_counts,
)

logger = setup_logger(__name__, level=logging.DEBUG, log_file="logs/app.log")


def load_config(config_path: str = "data/data_sources.json") -> dict:
    """
    Load configuration from JSON file.

    Args:
        config_path: Path to configuration file

    Returns:
        dict: Configuration dictionary

    Raises:
        FileNotFoundError: If config file not found
        json.JSONDecodeError: If config is invalid JSON
    """
    logger.info(f"Loading configuration from: {config_path}")

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        logger.info("Configuration loaded successfully")
        logger.info(f"Found {len(config.get('sources', []))} data sources")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file: {e}")
        raise


EXIT_GUARD_ABORT = 2


def collect_data(
    config_path: str = "data/data_sources.json",
    max_drop_pct: float = None,
    max_growth_pct: float = None,
    accept_count_change: bool = False,
):
    """
    Collect and merge data from all configured sources.

    Before the CSV is overwritten, per-city row counts are compared with the
    CSV currently on disk (in CI: the checked-out HEAD). If any city dropped by
    more than max_drop_pct (default 5) or grew by more than max_growth_pct
    (default 50) the run logs the numbers and exits non-zero without writing.
    accept_count_change=True skips that check (deliberate dataset changes).

    Returns:
        int: Exit code (0 success, 1 failure, 2 count guard abort)
    """
    logger.info("=" * 60)
    logger.info("Starting data collection")
    logger.info("=" * 60)

    try:
        # Load configuration
        config = load_config(config_path)
        output_file = config["output"]["file"]
        if max_drop_pct is None:
            max_drop_pct = float(config["output"].get("max_drop_pct", DEFAULT_MAX_DROP_PCT))
        if max_growth_pct is None:
            max_growth_pct = float(config["output"].get("max_growth_pct", DEFAULT_MAX_GROWTH_PCT))

        # Baseline must be read before anything touches the output file
        old_counts = read_committed_counts(output_file)

        # Create merger and collect data
        merger = DataMerger(config)
        df = merger.collect_and_merge()

        # Per-city count guard
        new_counts = city_counts(df)
        logger.info(f"Per-city rows, committed: {old_counts}")
        logger.info(f"Per-city rows, collected: {new_counts}")
        problems = check_city_counts(old_counts, new_counts, max_drop_pct, max_growth_pct)
        if problems:
            if accept_count_change:
                logger.warning("Count guard tripped but --accept-count-change was given:")
                for p in problems:
                    logger.warning(f"  - {p}")
            else:
                logger.error("=" * 60)
                logger.error(
                    f"COUNT GUARD ABORT: not writing {output_file} "
                    f"(max drop {max_drop_pct:g}%, max growth {max_growth_pct:g}%)"
                )
                for p in problems:
                    logger.error(f"  - {p}")
                logger.error("Re-run with --accept-count-change if this change is intended")
                logger.error("=" * 60)
                return EXIT_GUARD_ABORT

        # Save to CSV
        logger.info(f"Saving data to: {output_file}")

        # Ensure output directory exists
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(output_file, index=False, encoding="utf-8-sig")
        logger.info(f"Successfully saved {len(df)} records to {output_file}")

        # Validate the saved CSV
        logger.info("Validating CSV file")
        validation_result = validate_csv_file(output_file)

        if validation_result.is_valid():
            logger.info("CSV validation passed")
        else:
            logger.warning("CSV validation found issues:")
            for error in validation_result.errors[:10]:  # Show first 10 errors
                logger.warning(f"  - {error}")
            if len(validation_result.errors) > 10:
                logger.warning(
                    f"  ... and {len(validation_result.errors) - 10} more errors"
                )

        logger.info("=" * 60)
        logger.info("Data collection complete")
        logger.info("=" * 60)
        return 0

    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"Data collection failed: {e}", exc_info=True)
        logger.error("=" * 60)
        return 1


RETIRED_MESSAGE = (
    "is retired: the password login and the CI Google Maps sync are gone (the repo is public and "
    "must hold no Google credentials). Use 'sync-lists' on the ubuntu host with a logged-in "
    "Chrome profile; see deploy/ubuntu/README.md."
)


def retired_command(name: str) -> int:
    """authenticate / sync-maps used the Google password flow; they now only say so."""
    print(f"{name} {RETIRED_MESSAGE}", file=sys.stderr)
    return 1


def build_district_lists(config_path, boundaries, lists_dir, max_rows):
    """Assign districts by point-in-polygon and write data/lists/*.csv + index.json."""
    try:
        csv_file = load_config(config_path)["output"]["file"]
        index = build_lists(csv_file, boundaries, lists_dir, max_rows)
        logger.info(f"Wrote {len(index['lists'])} lists ({index['total_rows']} rows) to {lists_dir}")
        for item in index["lists"]:
            logger.info(f"  {item['list']}: {item['rows']} rows")
        return 0
    except Exception as e:
        logger.error(f"build-lists failed: {e}", exc_info=True)
        return 1


def plan_sync(lists_dir, state_path, out_path):
    """Print (and optionally write) the add/remove plan between data/lists and sync_state.json."""
    try:
        plan = make_plan(lists_dir, load_state(state_path))
        s = plan["summary"]
        print(f"sync plan: {s['lists']} lists, +{s['add']} add, -{s['remove']} remove, "
              f"={s['unchanged']} unchanged ({s['changed']} of the add/remove pairs are changed payloads)")
        if s["lists_new"]:
            print(f"  new lists: {len(s['lists_new'])}")
        if s["lists_gone"]:
            print(f"  lists to empty: {s['lists_gone']}")
        for name, p in plan["lists"].items():
            if p["add"] or p["remove"]:
                print(f"  {name}: +{len(p['add'])} -{len(p['remove'])} ={p['unchanged']} (changed {p['changed']})")
        if out_path:
            Path(out_path).parent.mkdir(parents=True, exist_ok=True)
            Path(out_path).write_text(
                json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
            print(f"plan written to {out_path}")
        return 0
    except Exception as e:
        logger.error(f"plan-sync failed: {e}", exc_info=True)
        return 1


def main():
    """
    Main CLI entrypoint.

    Returns:
        int: Exit code
    """
    parser = argparse.ArgumentParser(
        # no abbreviations: the ubuntu wrapper refuses --state by its full spelling, so --sta must not mean --state
        allow_abbrev=False,
        description="Taiwan Handicapped Parking Data Management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  collect       Collect data from all configured sources and merge
  authenticate  RETIRED (password login removed)
  sync-maps     RETIRED (use sync-lists)
  sync-lists    Sync data/lists to Google Maps saved lists via a logged-in Chrome profile (ubuntu)
  build-lists   Assign districts (point-in-polygon) and write data/lists/*.csv
  plan-sync     Print the add/remove plan between data/lists and data/sync_state.json

Examples:
  python scripts/main.py collect
  python scripts/main.py sync-lists --dry-run
  python scripts/main.py sync-lists --only "臺北市 中山區" --max-ops 230
  python scripts/main.py sync-lists --count "臺北市 中山區"
  python scripts/main.py build-lists
  python scripts/main.py plan-sync --out tmp/sync_plan.json

sync-lists exit codes: 0 done / nothing to do, 1 error, 2 partial (ops remain),
  3 relogin needed or blocked, 4 three consecutive failures.
        """,
    )

    parser.add_argument(
        "command",
        choices=["collect", "authenticate", "sync-maps", "sync-lists", "build-lists", "plan-sync"],
        help="Command to execute",
    )

    parser.add_argument(
        "--config",
        default="data/data_sources.json",
        help="Path to configuration file (default: data/data_sources.json)",
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    parser.add_argument(
        "--max-drop-pct",
        type=float,
        default=None,
        help=f"collect: abort if any city loses more than this %% of rows "
        f"(default {DEFAULT_MAX_DROP_PCT:g}, or output.max_drop_pct in the config)",
    )
    parser.add_argument(
        "--max-growth-pct",
        type=float,
        default=None,
        help=f"collect: abort if any city gains more than this %% of rows "
        f"(default {DEFAULT_MAX_GROWTH_PCT:g}, or output.max_growth_pct in the config)",
    )
    parser.add_argument(
        "--accept-count-change",
        action="store_true",
        help="collect: write the CSV even if the per-city count guard trips",
    )

    parser.add_argument("--boundaries", default="data/boundaries/tw_districts_tpe_ntpc.geojson",
                        help="build-lists: district polygon file")
    parser.add_argument("--lists-dir", default="data/lists",
                        help="build-lists / plan-sync: directory of per-district lists")
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS,
                        help="build-lists: max rows per list (default %(default)s)")
    parser.add_argument("--state", default=None,
                        help="plan-sync: what was last written to Google (default data/sync_state.json); "
                        f"sync-lists: its own state file (default {DEFAULT_STATE}, never the repo file)")
    parser.add_argument("--out", default=None, help="plan-sync: also write the full plan as JSON here")

    parser.add_argument("--profile", default=DEFAULT_PROFILE,
                        help="sync-lists: logged-in Chrome user-data-dir (default %(default)s)")
    parser.add_argument("--only", action="append", default=None, metavar="LIST",
                        help="sync-lists: only this list name (repeatable)")
    parser.add_argument("--max-ops", type=int, default=DEFAULT_MAX_OPS,
                        help="sync-lists: stop after this many add/remove ops (default %(default)s)")
    parser.add_argument("--dry-run", action="store_true",
                        help="sync-lists: print the plan counts, start no browser, write nothing")
    parser.add_argument("--headed", action=argparse.BooleanOptionalAction, default=True,
                        help="sync-lists: show the browser window (default; run under xvfb-run)")
    parser.add_argument("--run-id", default=None,
                        help="sync-lists: id written on every sync.log line (the wrapper script passes its own)")
    parser.add_argument("--count", default=None, metavar="LIST",
                        help="sync-lists: print how many places Google shows for this list (no writes)")

    args = parser.parse_args()

    # Execute command
    try:
        if args.command == "collect":
            exit_code = collect_data(
                config_path=args.config,
                max_drop_pct=args.max_drop_pct,
                max_growth_pct=args.max_growth_pct,
                accept_count_change=args.accept_count_change,
            )
        elif args.command == "build-lists":
            exit_code = build_district_lists(args.config, args.boundaries, args.lists_dir, args.max_rows)
        elif args.command == "plan-sync":
            exit_code = plan_sync(args.lists_dir, args.state or "data/sync_state.json", args.out)
        elif args.command == "sync-lists":
            exit_code = sync_lists(
                lists_dir=args.lists_dir,
                state=args.state or DEFAULT_STATE,
                profile=args.profile,
                only=args.only,
                max_ops=args.max_ops,
                dry_run=args.dry_run,
                headed=args.headed,
                count=args.count,
                run_id=args.run_id,
            )
        elif args.command in ("sync-maps", "authenticate"):
            exit_code = retired_command(args.command)
        else:
            logger.error(f"Unknown command: {args.command}")
            exit_code = 1

        sys.exit(exit_code)

    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
