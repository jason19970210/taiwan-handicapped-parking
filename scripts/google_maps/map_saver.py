"""
Google Maps location saver.
Syncs parking locations to a Google Maps saved list using Playwright.
"""

from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page
import pandas as pd
import time
import os
from pathlib import Path
from typing import Tuple, Optional

from ..utils.logger import setup_logger
from .selectors import GoogleMapsSelectors

logger = setup_logger(__name__, log_file='logs/playwright.log')


class GoogleMapsSaver:
    """
    Saves parking locations to Google Maps using browser automation.

    Loads authenticated session and systematically saves each location
    to a specified list, with duplicate detection and rate limiting.
    """

    def __init__(self):
        """
        Initialize saver with environment variables.

        Environment variables:
            AUTH_STATE_PATH: Path to authentication state
            HEADLESS: Whether to run browser in headless mode
            RATE_LIMIT_DELAY: Delay between requests (seconds)
        """
        self.auth_state_path = os.getenv('AUTH_STATE_PATH', '.github/auth/google_auth_state.json')
        self.headless = os.getenv('HEADLESS', 'true').lower() == 'true'
        self.rate_limit_delay = int(os.getenv('RATE_LIMIT_DELAY', '3'))
        self.list_name = "Taiwan Handicapped Parking"

        # Validate auth state exists
        if not Path(self.auth_state_path).exists():
            raise FileNotFoundError(
                f"Authentication state not found at: {self.auth_state_path}. "
                "Please run authentication first."
            )

    def sync_locations(self, csv_file: str) -> Tuple[int, int, int]:
        """
        Sync all locations from CSV to Google Maps.

        Args:
            csv_file: Path to CSV file with parking locations

        Returns:
            Tuple of (processed, skipped, errors) counts

        Raises:
            FileNotFoundError: If CSV file not found
            Exception: If sync fails
        """
        logger.info("=" * 60)
        logger.info(f"Starting Google Maps sync from: {csv_file}")
        logger.info("=" * 60)

        # Load CSV
        try:
            df = pd.read_csv(csv_file)
            logger.info(f"Loaded {len(df)} locations to sync")
            logger.info(f"CSV columns: {list(df.columns)}")
        except FileNotFoundError:
            logger.error(f"CSV file not found: {csv_file}")
            raise
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")
            raise

        # Initialize counters
        processed = 0
        skipped = 0
        errors = 0

        with sync_playwright() as p:
            browser = None
            try:
                # Launch browser
                logger.info("Launching Chromium browser")
                browser = p.chromium.launch(
                    headless=self.headless,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--disable-dev-shm-usage',
                        '--no-sandbox'
                    ]
                )

                # Load authentication state
                logger.info(f"Loading auth state from: {self.auth_state_path}")
                context = browser.new_context(
                    storage_state=self.auth_state_path,
                    viewport={'width': 1920, 'height': 1080}
                )

                page = context.new_page()

                # Ensure list exists
                logger.info("Ensuring list exists")
                self._ensure_list_exists(page)

                # Process each location
                logger.info("Starting location processing")
                logger.info(f"Rate limit delay: {self.rate_limit_delay}s")

                for idx, row in df.iterrows():
                    location = self._format_location(row)

                    try:
                        result = self._save_location(page, location, idx + 1, len(df))

                        if result == 'saved':
                            processed += 1
                        elif result == 'skipped':
                            skipped += 1
                        else:  # 'error'
                            errors += 1

                    except Exception as e:
                        errors += 1
                        logger.error(f"[{idx+1}/{len(df)}] ✗ Unexpected error for {location}: {e}")

                    # Rate limiting
                    if idx < len(df) - 1:  # Don't delay after last item
                        logger.debug(f"Waiting {self.rate_limit_delay}s before next request")
                        time.sleep(self.rate_limit_delay)

                # Summary
                logger.info("=" * 60)
                logger.info("Sync complete")
                logger.info(f"  Processed: {processed}")
                logger.info(f"  Skipped:   {skipped}")
                logger.info(f"  Errors:    {errors}")
                logger.info(f"  Total:     {len(df)}")
                logger.info("=" * 60)

            except Exception as e:
                logger.error("=" * 60)
                logger.error(f"Sync failed: {e}", exc_info=True)
                logger.error("=" * 60)
                raise

            finally:
                if browser:
                    browser.close()
                    logger.info("Browser closed")

        return processed, skipped, errors

    def _ensure_list_exists(self, page: Page) -> None:
        """
        Ensure the target list exists, create if needed.

        Args:
            page: Playwright page object
        """
        logger.info(f"Checking if list '{self.list_name}' exists")

        try:
            # Navigate to Google Maps
            page.goto('https://www.google.com/maps', wait_until='networkidle')
            page.wait_for_timeout(2000)

            # Open menu
            page.click(GoogleMapsSelectors.MENU_BUTTON, timeout=10000)
            page.wait_for_timeout(1000)

            # Check if list exists
            list_selector = GoogleMapsSelectors.list_by_name(self.list_name)
            if not page.is_visible(list_selector, timeout=5000):
                logger.info(f"List '{self.list_name}' not found, creating new list")

                # Click "Your lists"
                page.click(GoogleMapsSelectors.YOUR_LISTS, timeout=10000)
                page.wait_for_timeout(1000)

                # Click "Create list"
                page.click(GoogleMapsSelectors.CREATE_LIST_BUTTON, timeout=10000)
                page.wait_for_timeout(1000)

                # Enter list name
                page.fill(GoogleMapsSelectors.LIST_NAME_INPUT, self.list_name, timeout=10000)
                page.wait_for_timeout(500)

                # Create list
                create_button = GoogleMapsSelectors.button_with_text("Create")
                page.click(create_button, timeout=10000)
                page.wait_for_timeout(2000)

                logger.info(f"Created list: {self.list_name}")
            else:
                logger.info(f"List '{self.list_name}' already exists")

            # Close menu
            try:
                page.click(GoogleMapsSelectors.CLOSE_BUTTON, timeout=5000)
            except:
                # Click outside menu to close
                page.mouse.click(100, 100)

            page.wait_for_timeout(1000)

        except Exception as e:
            logger.error(f"Error ensuring list exists: {e}")
            raise

    def _save_location(self, page: Page, location: str, current: int, total: int) -> str:
        """
        Save a single location to Google Maps.

        Args:
            page: Playwright page object
            location: Formatted location string
            current: Current item number
            total: Total items

        Returns:
            str: 'saved', 'skipped', or 'error'
        """
        logger.debug(f"[{current}/{total}] Processing: {location}")

        try:
            # Search for location
            page.fill(GoogleMapsSelectors.SEARCH_INPUT, location, timeout=10000)
            page.press(GoogleMapsSelectors.SEARCH_INPUT, 'Enter')
            page.wait_for_timeout(3000)

            # Check if save button exists
            if not page.is_visible(GoogleMapsSelectors.SAVE_BUTTON, timeout=5000):
                logger.warning(f"[{current}/{total}] ○ Location not found: {location}")
                return 'error'

            # Check if already saved
            save_button = page.locator(GoogleMapsSelectors.SAVE_BUTTON).first
            aria_label = save_button.get_attribute('aria-label') or ''

            if 'Saved' in aria_label or 'saved' in aria_label:
                logger.info(f"[{current}/{total}] ○ Already saved: {location}")
                return 'skipped'

            # Click save button
            logger.debug(f"[{current}/{total}] Saving location: {location}")
            save_button.click(timeout=10000)
            page.wait_for_timeout(1500)

            # Click list name to save to specific list
            list_selector = GoogleMapsSelectors.list_by_name(self.list_name)
            page.click(list_selector, timeout=10000)
            page.wait_for_timeout(1000)

            logger.info(f"[{current}/{total}] ✓ Saved: {location}")
            return 'saved'

        except Exception as e:
            logger.error(f"[{current}/{total}] ✗ Error saving {location}: {e}")
            return 'error'

    def _format_location(self, row: pd.Series) -> str:
        """
        Format location string from CSV row.

        Args:
            row: CSV row with location data

        Returns:
            str: Formatted location string
        """
        # Build location string from available fields
        parts = []

        road = str(row.get('road', '')).strip()
        area = str(row.get('area', '')).strip()
        city = str(row.get('city', '')).strip()

        if road:
            parts.append(road)
        if area:
            parts.append(area)
        if city:
            parts.append(city)

        # Add Taiwan for better search results
        parts.append("Taiwan")

        return ', '.join(parts)
