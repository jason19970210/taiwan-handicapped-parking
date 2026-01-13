"""
Google Maps authentication handler.
Handles login automation and persistent authentication state.
"""

from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page
import os
from pathlib import Path
from typing import Optional

from ..utils.logger import setup_logger
from .selectors import GoogleMapsSelectors

logger = setup_logger(__name__, log_file='logs/playwright.log')


class GoogleMapsAuthenticator:
    """
    Handles Google Maps authentication using Playwright.

    Authenticates with Google account and saves authentication state
    for reuse in subsequent sessions.
    """

    def __init__(self):
        """
        Initialize authenticator with environment variables.

        Environment variables:
            GOOGLE_EMAIL: Google account email
            GOOGLE_PASSWORD: Google account password
            AUTH_STATE_PATH: Path to save authentication state
            HEADLESS: Whether to run browser in headless mode
        """
        self.email = os.getenv('GOOGLE_EMAIL')
        self.password = os.getenv('GOOGLE_PASSWORD')
        self.auth_state_path = os.getenv('AUTH_STATE_PATH', '.github/auth/google_auth_state.json')
        self.headless = os.getenv('HEADLESS', 'true').lower() == 'true'

        # Validate credentials
        if not self.email or not self.password:
            raise ValueError("GOOGLE_EMAIL and GOOGLE_PASSWORD environment variables must be set")

    def authenticate(self) -> None:
        """
        Perform Google Maps authentication and save state.

        Raises:
            ValueError: If credentials are not set
            Exception: If authentication fails
        """
        logger.info("=" * 60)
        logger.info("Starting Google Maps authentication")
        logger.info(f"Headless mode: {self.headless}")
        logger.info(f"Auth state path: {self.auth_state_path}")
        logger.info("=" * 60)

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

                # Create context with anti-detection settings
                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )

                # Remove webdriver flag
                context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                """)
                logger.info("Browser context created with anti-detection settings")

                # Create page and navigate
                page = context.new_page()
                logger.info("Navigating to Google Maps")
                page.goto('https://www.google.com/maps', wait_until='networkidle')
                page.wait_for_timeout(2000)

                # Click sign in button
                logger.info("Looking for sign in button")
                try:
                    page.click(GoogleMapsSelectors.SIGN_IN_BUTTON, timeout=10000)
                    logger.info("Clicked sign in button")
                except Exception as e:
                    logger.warning(f"Could not find sign in button: {e}")
                    logger.info("Attempting alternative sign in method")
                    # Try alternative selectors
                    page.click('a[href*="accounts.google.com"]', timeout=10000)

                page.wait_for_timeout(2000)

                # Enter email
                masked_email = f"{self.email[:3]}***@{self.email.split('@')[1]}" if '@' in self.email else "***"
                logger.info(f"Entering email: {masked_email}")
                page.wait_for_selector(GoogleMapsSelectors.EMAIL_INPUT, timeout=10000)
                page.fill(GoogleMapsSelectors.EMAIL_INPUT, self.email)
                page.click(GoogleMapsSelectors.NEXT_BUTTON)
                page.wait_for_timeout(3000)

                # Enter password
                logger.info("Entering password")
                try:
                    page.wait_for_selector(GoogleMapsSelectors.PASSWORD_INPUT, timeout=10000)
                    page.fill(GoogleMapsSelectors.PASSWORD_INPUT, self.password)
                    page.click(GoogleMapsSelectors.NEXT_BUTTON)
                    logger.info("Password submitted")
                except Exception as e:
                    logger.error(f"Error entering password: {e}")
                    raise

                # Wait for authentication to complete
                logger.info("Waiting for authentication to complete")
                try:
                    page.wait_for_url('**/maps**', timeout=30000)
                    logger.info("Authentication successful - redirected to Maps")
                except Exception as e:
                    logger.warning(f"Did not detect URL change to maps: {e}")
                    # Check if we're already on maps page
                    current_url = page.url
                    if 'maps' in current_url:
                        logger.info(f"Already on Maps page: {current_url}")
                    else:
                        logger.error(f"Authentication may have failed. Current URL: {current_url}")
                        raise Exception("Authentication failed - not on Maps page")

                # Additional wait to ensure full page load
                page.wait_for_timeout(5000)

                # Save authentication state
                logger.info("Saving authentication state")
                Path(self.auth_state_path).parent.mkdir(parents=True, exist_ok=True)
                context.storage_state(path=self.auth_state_path)
                logger.info(f"Auth state saved to: {self.auth_state_path}")

                logger.info("=" * 60)
                logger.info("Authentication complete successfully")
                logger.info("=" * 60)

            except Exception as e:
                logger.error("=" * 60)
                logger.error(f"Authentication failed: {e}", exc_info=True)
                logger.error("=" * 60)
                raise

            finally:
                if browser:
                    browser.close()
                    logger.info("Browser closed")

    def is_authenticated(self) -> bool:
        """
        Check if valid authentication state exists.

        Returns:
            bool: True if auth state file exists
        """
        return Path(self.auth_state_path).exists()

    def clear_auth_state(self) -> None:
        """
        Clear saved authentication state.

        Useful for troubleshooting or forcing re-authentication.
        """
        auth_path = Path(self.auth_state_path)
        if auth_path.exists():
            auth_path.unlink()
            logger.info(f"Cleared auth state from: {self.auth_state_path}")
        else:
            logger.info("No auth state to clear")
