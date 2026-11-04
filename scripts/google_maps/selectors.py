"""
Every Google Maps selector, URL and page-text marker used by the saved-list sync.

All of these are UNVERIFIED against the live site until the first capacity run
(`sync-lists --only "<list>" --max-ops 230`). Google changes this markup without
notice; when an op fails with "... not found", fix the constant here and nowhere
else (scripts/lists/maps_sync.py holds no selector strings).

Every URL carries hl=en so aria-labels are English whatever the profile locale
is. Traditional Chinese variants are listed as a second alternative where a
label is known to differ.
"""


class GoogleMapsSelectors:
    # ---- URLs --------------------------------------------------------------
    HOME_URL = "https://www.google.com/maps?hl=en"
    # {lat}/{lon} are formatted to 6 decimals (the same precision as the point id)
    PLACE_URL = "https://www.google.com/maps/search/?api=1&query={lat},{lon}&hl=en"

    # ---- session state -----------------------------------------------------
    # Signed in = ACCOUNT_BUTTON present and SIGN_IN_LINK absent.
    ACCOUNT_BUTTON = (
        'a[aria-label^="Google Account"], a[aria-label^="Google 帳戶"], '
        'a[href*="accounts.google.com/SignOutOptions"]'
    )
    SIGN_IN_LINK = (
        'a[aria-label="Sign in"], a[aria-label="登入"], '
        'a[href*="accounts.google.com/ServiceLogin"]'
    )

    # ---- blocked / interstitial detection ----------------------------------
    CAPTCHA = 'iframe[src*="recaptcha"], form#captcha-form, #recaptcha'
    CONSENT_FORM = 'form[action*="consent.google.com"]'
    BODY = "body"
    # page.url substrings
    BLOCKED_URL_MARKERS = ("/sorry/", "consent.google.com")
    SIGNED_OUT_URL_MARKERS = ("accounts.google.com",)
    # lower-cased page text substrings
    BLOCKED_TEXT_MARKERS = (
        "unusual traffic",
        "not a robot",
        "異常流量",
        "我不是機器人",
        "before you continue to google",
    )

    # ---- place panel and Save dialog --------------------------------------
    # Button in the place panel; exactly ONE must match (the sync refuses to click otherwise).
    # Reads "Save" when the place is on none of our lists and "Saved in ..." otherwise; both open
    # the same dialog. A bare aria-label "Saved" is deliberately not matched: that is the left rail.
    SAVE_BUTTON = (
        'button[data-value="Save"], button[aria-label="Save"], '
        'button[aria-label^="Saved in"], button[aria-label="儲存"], '
        'button[aria-label^="已儲存於"]'
    )
    # The open dialog/menu. List rows and the mutation controls are looked up only inside the one
    # dialog that contains them (exactly one such dialog must exist).
    SAVE_DIALOG = '[role="dialog"], [role="menu"]'
    # One row per list in the Save dialog; aria-checked must be exactly "true" or "false".
    # The list name is the first line of the row's text; exactly one row must match the name.
    LIST_ITEM = '[role="menuitemcheckbox"]'
    CHECKED_ATTR = "aria-checked"
    CHECKED_VALUE = "true"
    UNCHECKED_VALUE = "false"

    # ---- list creation: New list is in the Save dialog; the name form is in the dialog holding the input
    NEW_LIST_BUTTON = 'button:has-text("New list"), button:has-text("新增清單")'
    LIST_NAME_INPUT = 'input[aria-label="List name"], input[aria-label="清單名稱"]'
    CREATE_LIST_CONFIRM = 'button:has-text("Create"), button:has-text("建立")'

    # ---- saved-lists view (used only by --count) ---------------------------
    SAVED_NAV = 'button[aria-label="Saved"], button[aria-label="已儲存"]'
    MENU_BUTTON = 'button[aria-label="Menu"], button[aria-label="選單"]'
    MENU_SAVED_ITEM = 'button:has-text("Saved"), a:has-text("Saved"), button:has-text("已儲存")'
    # one row per list in the Saved tab; matched by exact name line, see maps_sync.count_list
    SAVED_LIST_ROW = "button"

    @staticmethod
    def place_url(lat: float, lon: float) -> str:
        return GoogleMapsSelectors.PLACE_URL.format(lat=f"{float(lat):.6f}", lon=f"{float(lon):.6f}")
