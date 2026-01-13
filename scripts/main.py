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
from scripts.google_maps.authenticator import GoogleMapsAuthenticator
from scripts.google_maps.map_saver import GoogleMapsSaver
from scripts.utils.logger import setup_logger
from scripts.utils.csv_validator import validate_csv_file

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


def collect_data():
    """
    Collect and merge data from all configured sources.

    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    logger.info("=" * 60)
    logger.info("Starting data collection")
    logger.info("=" * 60)

    try:
        # Load configuration
        config = load_config()

        # Create merger and collect data
        merger = DataMerger(config)
        df = merger.collect_and_merge()

        # Save to CSV
        output_file = config["output"]["file"]
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


def authenticate_google():
    """
    Authenticate with Google Maps and save authentication state.

    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    logger.info("=" * 60)
    logger.info("Starting Google authentication")
    logger.info("=" * 60)

    try:
        authenticator = GoogleMapsAuthenticator()
        authenticator.authenticate()

        logger.info("=" * 60)
        logger.info("Google authentication complete")
        logger.info("=" * 60)
        return 0

    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"Google authentication failed: {e}", exc_info=True)
        logger.error("=" * 60)
        return 1


def sync_to_maps():
    """
    Sync parking locations to Google Maps.

    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    logger.info("=" * 60)
    logger.info("Starting Google Maps sync")
    logger.info("=" * 60)

    try:
        # Load configuration to get CSV file path
        config = load_config()
        csv_file = config["output"]["file"]

        # Check if CSV file exists
        if not Path(csv_file).exists():
            logger.error(f"CSV file not found: {csv_file}")
            logger.error("Please run 'collect' command first")
            return 1

        # Sync to Google Maps
        saver = GoogleMapsSaver()
        processed, skipped, errors = saver.sync_locations(csv_file)

        # Determine exit code
        if errors > 0:
            logger.warning(f"Sync completed with {errors} errors")
            return 1
        else:
            logger.info("Sync completed successfully")
            return 0

    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"Google Maps sync failed: {e}", exc_info=True)
        logger.error("=" * 60)
        return 1


def main():
    """
    Main CLI entrypoint.

    Returns:
        int: Exit code
    """
    parser = argparse.ArgumentParser(
        description="Taiwan Handicapped Parking Data Management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  collect       Collect data from all configured sources and merge
  authenticate  Authenticate with Google Maps and save session
  sync-maps     Sync parking locations to Google Maps

Examples:
  python scripts/main.py collect
  python scripts/main.py authenticate
  python scripts/main.py sync-maps

Environment Variables:
  GOOGLE_EMAIL      Google account email (for authenticate, sync-maps)
  GOOGLE_PASSWORD   Google account password (for authenticate, sync-maps)
  AUTH_STATE_PATH   Path to save auth state (default: .github/auth/google_auth_state.json)
  HEADLESS          Run browser in headless mode (default: true)
  RATE_LIMIT_DELAY  Delay between requests in seconds (default: 3)
        """,
    )

    parser.add_argument(
        "command",
        choices=["collect", "authenticate", "sync-maps"],
        help="Command to execute",
    )

    parser.add_argument(
        "--config",
        default="data/data_sources.json",
        help="Path to configuration file (default: data/data_sources.json)",
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Execute command
    try:
        if args.command == "collect":
            exit_code = collect_data()
        elif args.command == "authenticate":
            exit_code = authenticate_google()
        elif args.command == "sync-maps":
            exit_code = sync_to_maps()
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
