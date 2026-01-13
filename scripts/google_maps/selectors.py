"""
UI selectors for Google Maps automation.
Centralized selectors for easy maintenance when Google Maps UI changes.
"""


class GoogleMapsSelectors:
    """
    Centralized UI selectors for Google Maps Playwright automation.

    These selectors may need to be updated if Google Maps changes its UI.
    Using a centralized class makes maintenance easier.
    """

    # Main navigation
    MENU_BUTTON = 'button[aria-label="Menu"]'
    SIGN_IN_BUTTON = 'button[aria-label*="Sign in"]'

    # Search
    SEARCH_INPUT = 'input#searchboxinput'
    SEARCH_BUTTON = 'button[aria-label*="Search"]'

    # Location actions
    SAVE_BUTTON = 'button[aria-label*="Save"]'
    SAVED_INDICATOR = 'button[aria-label*="Saved"]'

    # Lists management
    YOUR_LISTS = 'text="Your lists"'
    CREATE_LIST_BUTTON = 'button:has-text("Create list")'
    LIST_NAME_INPUT = 'input[placeholder*="name"]'

    # Authentication
    EMAIL_INPUT = 'input[type="email"]'
    PASSWORD_INPUT = 'input[type="password"]'
    NEXT_BUTTON = 'button:has-text("Next")'

    # Common
    CLOSE_BUTTON = 'text="Close"'

    @staticmethod
    def list_by_name(list_name: str) -> str:
        """
        Generate selector for a specific list by name.

        Args:
            list_name: Name of the list

        Returns:
            str: Selector for the list
        """
        return f'text="{list_name}"'

    @staticmethod
    def button_with_text(text: str) -> str:
        """
        Generate selector for a button with specific text.

        Args:
            text: Button text

        Returns:
            str: Selector for the button
        """
        return f'button:has-text("{text}")'
