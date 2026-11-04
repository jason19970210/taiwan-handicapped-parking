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

    # ---- place panel and Save menu (probed live, English UI, hl=en) ----------
    # Place-page button; exactly ONE must match (the sync refuses to click otherwise). Its
    # data-value is "Saved" (aria-label "Saved") when the place is already on some list and
    # presumably "Save" when on none. The left-rail "Saved" item is not such a button.
    SAVE_BUTTON = 'button[data-value^="Save"]'   # live values: "Save", "Saved", "Saved (2)" (on two lists)
    # Clicking it opens ONE div[role="menu"][aria-label="Save in your lists"]. The two role=dialog
    # elements on the place page are unrelated. List rows and "New list" are looked up only inside
    # the one menu that contains rows.
    SAVE_DIALOG = '[role="menu"]'
    # One row per list: div[role="menuitemradio"] with aria-checked exactly "true" or "false".
    # Row text = glyph lines (private-use icons, may be empty), the name, then a
    # "Private/Shared · N places" line. A row matches a list when ONE of its lines, stripped, is
    # exactly the list name; never a substring and never "the first line". Exactly one row may match.
    LIST_ITEM = '[role="menuitemradio"], [role="menuitemcheckbox"]'
    CHECKED_ATTR = "aria-checked"
    CHECKED_VALUE = "true"
    UNCHECKED_VALUE = "false"

    # ---- list creation ------------------------------------------------------
    # "New list" is a button inside the Save menu (text " New list" after an icon glyph, empty aria-label).
    NEW_LIST_BUTTON = 'button:has-text("New list"), button:has-text("新增清單")'
    # It opens a form in a role=dialog: a text input with no aria-label/placeholder, a "Choose icon"
    # button, two Close buttons and button[data-value="Create"]. The form dialog is the ONE
    # CREATE_DIALOG that contains the Create button; the input must be unique inside it (inputs outside
    # the dialog, a combobox and textareas, do not count).
    CREATE_DIALOG = '[role="dialog"]'
    LIST_NAME_INPUT = 'input[type="text"]'   # looked up inside the Create dialog locator, so no dialog prefix
    CREATE_LIST_CONFIRM = 'button[data-value="Create"], button[aria-label="Create"], button[aria-label="建立"]'

    @staticmethod
    def place_url(lat: float, lon: float) -> str:
        return GoogleMapsSelectors.PLACE_URL.format(lat=f"{float(lat):.6f}", lon=f"{float(lon):.6f}")
