"""
Google Maps saved-list sync (ubuntu side).

Reads data/lists (desired) and a local sync state file (what was last written),
turns the difference into add/remove operations on the 33 lists named in
data/lists/index.json, and applies them through a logged-in Chrome profile with
Playwright. No credential is ever used: the profile directory is the only
secret and it never leaves the machine.

Several point ids of one list can share one Google place (same lat/lon at 6
decimals, which is what the place URL carries). The unit of work against Google
is therefore the place group (list, place key); state stays per point id:
  - a group is added once, when any of its desired ids is not recorded yet;
  - Google membership is removed only when no desired id is left in the group;
  - ids that vanish while the group still has desired ids are only forgotten in
    state ("forget": no Google op).

Exit codes (see run_sync):
  0 done / nothing to do      2 partial (ops remain)
  1 other error               3 relogin needed or blocked
  4 three consecutive failures

Layers, so tests never need a browser:
  build_tasks()   plan -> ordered task list (pure)
  ops over a Page check_session / ensure_list / add_point / remove_point / count_list
                  (the only code that touches selectors, all from GoogleMapsSelectors)
  run_sync()      the loop: pacing, failure policy, atomic state, JSON log (takes any ops object)
  sync_lists()    CLI entry: dry-run, count, real run with launch_persistent_context
"""

import contextlib
import json
import os
import random
import re
import sys
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from scripts.google_maps.selectors import GoogleMapsSelectors as S
from scripts.lists.sync_plan import load_state, make_plan

# ---- exit codes ----------------------------------------------------------------
EXIT_OK = 0
EXIT_ERROR = 1
EXIT_PARTIAL = 2
EXIT_RELOGIN = 3
EXIT_FAILURES = 4

# ---- defaults ------------------------------------------------------------------
DEFAULT_PROFILE = "~/.config/thp-maps-profile"
DEFAULT_STATE = "~/.local/state/thp-maps/sync_state.json"
DEFAULT_MAX_OPS = 300
PACE_SECONDS = (6.0, 12.0)      # random pause between ops
LONG_PAUSE_EVERY = 50           # ops
LONG_PAUSE_SECONDS = 30.0
MAX_CONSECUTIVE_FAILURES = 3
VIEWPORT = {"width": 1440, "height": 900}
LAUNCH_ARGS = ["--password-store=basic"]
MAX_FAILURE_SHOTS = 5


# ---- errors --------------------------------------------------------------------
class SyncError(Exception):
    """Base class; reason is the text written to the log."""


class ReloginNeeded(SyncError):
    pass


class Blocked(SyncError):
    pass


class OpFailed(SyncError):
    pass


# ---- state ---------------------------------------------------------------------
def write_state_atomic(path: Path, state: Dict[str, Any]) -> None:
    """Write state as tmp file + fsync + os.replace: a crash leaves the old file or the new one, never half."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=1, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            with contextlib.suppress(OSError):
                tmp.unlink()


def place_key(lat: Any, lon: Any) -> str:
    """The coordinates exactly as written into the place URL (6 decimals)."""
    return f"{float(lat):.6f},{float(lon):.6f}"


# ---- plan -> tasks ---------------------------------------------------------------
@dataclass
class Task:
    list: str
    op: str                      # "ensure" | "add" | "remove" | "forget"
    point: Dict[str, Any] = field(default_factory=dict)       # representative: lat, lon, point_id, ...
    points: List[Dict[str, Any]] = field(default_factory=list)  # every id this task settles
    key: str = ""

    @property
    def point_id(self) -> Optional[str]:
        return self.point.get("point_id")

    @property
    def ids(self) -> List[str]:
        return [p["point_id"] for p in self.points]


def load_index(lists_dir: str) -> List[Dict[str, Any]]:
    return json.loads((Path(lists_dir) / "index.json").read_text(encoding="utf-8"))["lists"]


def load_index_names(lists_dir: str) -> List[str]:
    return [e["list"] for e in load_index(lists_dir)]


def _desired_place_keys(lists_dir: str, entry: Dict[str, Any]) -> Dict[str, str]:
    """point_id -> place key for every row of one list file."""
    df = pd.read_csv(Path(lists_dir) / entry["file"], encoding="utf-8-sig", keep_default_na=False, dtype=str)
    return {r["point_id"]: place_key(r["dd_lat"], r["dd_long"]) for r in df.to_dict("records")}


def first_desired_point(lists_dir: str, name: str) -> Optional[Tuple[float, float]]:
    """(lat, lon) of the first row of one list's file, or None when the list has no points."""
    entry = next(e for e in load_index(lists_dir) if e["list"] == name)
    df = pd.read_csv(Path(lists_dir) / entry["file"], encoding="utf-8-sig", keep_default_na=False, dtype=str)
    if df.empty:
        return None
    row = df.iloc[0]
    return float(row["dd_lat"]), float(row["dd_long"])


def build_tasks(lists_dir: str, state: Dict[str, Any], only: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    """
    Plan as an ordered task list, restricted to the lists named in index.json.

    make_plan also emits remove-all entries for lists that are in the state but no
    longer in the index ("lists_gone"); this program never deletes lists and never
    opens a list it does not own, so those are dropped here.

    make_plan works per point id; Google works per place, so the plan is regrouped
    by (list, place key). Per list the order is: ensure (when the list is not yet
    recorded and has adds), then removes/forgets, then adds.
      add     group has an id to write (new, or changed payload): tick the place once
      remove  group has ids in state and no desired id at that place: untick once
      forget  ids left state's desired set but their place is still desired: state only
    A changed id is not a remove: its place stays wanted, so it becomes an add whose
    tick is already there.

    Raises ValueError when --only names a list that is not in the index.
    """
    entries = load_index(lists_dir)
    names = [e["list"] for e in entries]
    wanted = list(only) if only else None
    if wanted:
        unknown = [n for n in wanted if n not in names]
        if unknown:
            raise ValueError(f"not a list in {lists_dir}/index.json: {unknown}")
    selected = [e for e in entries if wanted is None or e["list"] in wanted]

    plan = make_plan(lists_dir, state)
    tasks: List[Task] = []
    skipped: List[Dict[str, Any]] = []
    adds = removes = unchanged = groups_multi = extra_ids = places_to_add = 0
    for entry in selected:
        name = entry["list"]
        p = plan["lists"][name]
        adds += len(p["add"])
        removes += len(p["remove"])
        unchanged += p["unchanged"]
        desired = _desired_place_keys(lists_dir, entry)
        desired_keys = set(desired.values())
        per_key = Counter(desired.values())
        groups_multi += sum(1 for n in per_key.values() if n > 1)
        extra_ids += sum(n - 1 for n in per_key.values() if n > 1)

        add_groups: Dict[str, List[Dict[str, Any]]] = {}
        for a in p["add"]:
            add_groups.setdefault(place_key(a["lat"], a["lon"]), []).append(a)
        remove_groups: Dict[str, List[Dict[str, Any]]] = {}
        for r in p["remove"]:
            if r.get("reason") == "changed":
                continue                      # the add side of this id keeps the place wanted
            if r.get("lat") is None or r.get("lon") is None:
                skipped.append({"list": name, "point_id": r["point_id"], "reason": "no coordinates in state"})
                continue
            remove_groups.setdefault(place_key(r["lat"], r["lon"]), []).append(r)

        places_to_add += len(add_groups)
        exists = bool(state.get("lists", {}).get(name, {}).get("exists"))
        if add_groups and not exists:
            first = next(iter(add_groups.values()))[0]
            tasks.append(Task(name, "ensure", first, [], place_key(first["lat"], first["lon"])))
        for key, group in remove_groups.items():
            op = "forget" if key in desired_keys else "remove"
            tasks.append(Task(name, op, group[0], group, key))
        for key, group in add_groups.items():
            tasks.append(Task(name, "add", group[0], group, key))
    return {
        "tasks": tasks,
        "lists": len(selected),
        "adds": adds,
        "removes": removes,
        "unchanged": unchanged,
        "places_to_add": places_to_add,
        "groups_multi": groups_multi,
        "extra_ids": extra_ids,
        "skipped": skipped,
        "ignored_state_lists": sorted(n for n in state.get("lists", {}) if n not in set(names)),
    }


# ---- JSON log ----------------------------------------------------------------------
class RunLog:
    """One JSON line per event, appended to the log file and echoed to stdout."""

    def __init__(self, path: Optional[Path], run_id: str, echo: bool = True):
        self.path = Path(path) if path else None
        self.run_id = run_id
        self.echo = echo

    def event(self, **fields: Any) -> None:
        rec = {"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), "run_id": self.run_id}
        rec.update(fields)
        line = json.dumps(rec, ensure_ascii=False)
        if self.path:
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with open(self.path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
            except OSError as e:               # never let logging take the run down; stdout still has it
                print(f"sync.log write failed: {e}", file=sys.stderr)
        if self.echo:
            print(line, flush=True)


# ---- page helpers ------------------------------------------------------------------
def _poll(page, fn: Callable[[], Any], timeout_ms: int = 10000, step_ms: int = 500):
    """Call fn until it returns something truthy or the timeout passes; returns the last value."""
    waited = 0
    value = fn()
    while not value and waited < timeout_ms:
        page.wait_for_timeout(step_ms)
        waited += step_ms
        value = fn()
    return value


def _count(page, selector: str) -> int:
    return page.locator(selector).count()


def check_blocked(page) -> None:
    """Raise Blocked / ReloginNeeded when the page is a sign-in, consent, captcha or 'unusual traffic' page."""
    url = page.url or ""
    if any(m in url for m in S.BLOCKED_URL_MARKERS):
        raise Blocked(f"blocked: {url}")
    if any(m in url for m in S.SIGNED_OUT_URL_MARKERS):
        raise ReloginNeeded(f"relogin needed: redirected to {url}")
    if _count(page, S.CAPTCHA):
        raise Blocked("blocked: captcha on page")
    if _count(page, S.CONSENT_FORM):
        raise Blocked("blocked: consent interstitial")
    try:
        text = (page.locator(S.BODY).inner_text(timeout=3000) or "").lower()
    except Exception:
        text = ""
    for marker in S.BLOCKED_TEXT_MARKERS:
        if marker in text:
            raise Blocked(f"blocked: page says '{marker}'")
    if _count(page, S.SIGN_IN_LINK):
        raise ReloginNeeded("relogin needed: sign-in link on page")


def _fail(page, message: str):
    """Report a failed op, but a sign-in/consent/captcha page takes precedence (exit 3, not a selector failure)."""
    check_blocked(page)
    raise OpFailed(message)


def _one(page, loc, what: str, const: str):
    """The locator must match exactly one element; anything else is a failure naming the count."""
    n = loc.count()
    if n != 1:
        _fail(page, f"{what}: expected exactly 1 match, found {n} ({const})")
    return loc


def check_session(page) -> None:
    """Signed in = account button present and no sign-in link. Anything else raises."""
    page.goto(S.HOME_URL, wait_until="domcontentloaded")
    _poll(page, lambda: _count(page, S.ACCOUNT_BUTTON) or _count(page, S.SIGN_IN_LINK), 15000)
    check_blocked(page)
    if not _count(page, S.ACCOUNT_BUTTON):
        raise ReloginNeeded("relogin needed: no account button on google.com/maps")


def _container_with(page, container: str, inner: str):
    """The element(s) matching `container` that contain `inner`."""
    return page.locator(container, has=page.locator(inner))


def _save_dialog(page):
    return _one(page, _container_with(page, S.SAVE_DIALOG, S.LIST_ITEM), "save menu", "SAVE_DIALOG")


def _open_save_dialog(page, lat: float, lon: float) -> None:
    page.goto(S.place_url(lat, lon), wait_until="domcontentloaded")
    if not _poll(page, lambda: _count(page, S.SAVE_BUTTON), 15000):
        _fail(page, "save button not found (SAVE_BUTTON)")
    check_blocked(page)
    _one(page, page.locator(S.SAVE_BUTTON), "save button", "SAVE_BUTTON").click(timeout=10000)
    if not _poll(page, lambda: _container_with(page, S.SAVE_DIALOG, S.LIST_ITEM).count(), 8000):
        _fail(page, "save dialog did not open (SAVE_DIALOG, LIST_ITEM)")
    check_blocked(page)


def _close_dialog(page) -> None:
    with contextlib.suppress(Exception):
        page.keyboard.press("Escape")


def _find_list_item(page, name: str):
    """
    The one Save-menu row that has a text line (stripped) exactly equal to the list name, or None when
    there is none. Rows carry icon-glyph lines before the name and a "Private/Shared · N places" line
    after it, so neither the first line nor a substring is used. Rows are read only inside the open
    menu. More than one match is a failure.
    """
    rows = _save_dialog(page).locator(S.LIST_ITEM)
    found = []
    for i in range(rows.count()):
        row = rows.nth(i)
        if name in [ln.strip() for ln in (row.inner_text() or "").splitlines()]:
            found.append(row)
    if len(found) > 1:
        _fail(page, f"list '{name}': expected exactly 1 row in the save dialog, found {len(found)} (LIST_ITEM)")
    return found[0] if found else None


def _require_list_item(page, name: str):
    row = _find_list_item(page, name)
    if row is None:
        _fail(page, f"list '{name}': expected exactly 1 row in the save dialog, found 0 (LIST_ITEM)")
    return row


def _is_checked(page, row) -> bool:
    """Only an explicit aria-checked of "true" or "false" counts; anything else is a failure."""
    v = row.get_attribute(S.CHECKED_ATTR)
    if v == S.CHECKED_VALUE:
        return True
    if v == S.UNCHECKED_VALUE:
        return False
    _fail(page, f"list row has {S.CHECKED_ATTR}={v!r}, expected 'true' or 'false' (CHECKED_ATTR)")


def _read_checked(page, name: str, lat: float, lon: float) -> bool:
    """Open the place fresh and report whether our list is ticked; the dialog is closed again."""
    _open_save_dialog(page, lat, lon)
    try:
        return _is_checked(page, _require_list_item(page, name))
    finally:
        _close_dialog(page)


def _verify(page, name: str, lat: float, lon: float, expected: bool, attempts: int = 3) -> None:
    """Reload the place and check the tick persisted (the dialog may close itself after a click)."""
    seen = None
    for k in range(attempts):
        page.wait_for_timeout(2000 * 2 ** k)      # 2, 4, 8 s: Google can lag before a fresh view shows the change
        seen = _read_checked(page, name, lat, lon)
        if seen == expected:
            return
    raise OpFailed(f"verify failed: '{name}' ticked={seen}, expected {expected}")


def ensure_list(page, name: str, lat: float, lon: float) -> str:
    """
    Make sure a list with exactly this name exists. Opens the Save dialog of the
    anchor place (lat, lon); if the list is missing it is created from the dialog
    ('New list'), which also saves the anchor place into it. That place is a point
    the same run adds next, and add_point treats an already-ticked list as done.
    Returns "exists" or "created".
    """
    _open_save_dialog(page, lat, lon)
    if _find_list_item(page, name) is not None:
        _close_dialog(page)
        return "exists"
    dialog = _save_dialog(page)
    if not dialog.locator(S.NEW_LIST_BUTTON).count():
        _fail(page, "new list button not found (NEW_LIST_BUTTON)")
    _one(page, dialog.locator(S.NEW_LIST_BUTTON), "new list button", "NEW_LIST_BUTTON").click(timeout=10000)
    if not _poll(page, lambda: _container_with(page, S.CREATE_DIALOG, S.CREATE_LIST_CONFIRM).count(), 8000):
        _fail(page, "list name form did not open (CREATE_DIALOG, CREATE_LIST_CONFIRM)")
    form = _one(page, _container_with(page, S.CREATE_DIALOG, S.CREATE_LIST_CONFIRM),
                "list name dialog", "CREATE_DIALOG")
    _one(page, form.locator(S.LIST_NAME_INPUT), "list name input", "LIST_NAME_INPUT").fill(name, timeout=10000)
    _one(page, form.locator(S.CREATE_LIST_CONFIRM), "create button", "CREATE_LIST_CONFIRM").click(timeout=10000)
    # Google shows a new list only after a delay (live: not on the first fresh view 2 s later), so poll fresh views.
    # If it never shows, the list may still exist: the next run finds it and adopts it instead of creating another.
    for wait_ms in (3000, 6000, 12000):
        page.wait_for_timeout(wait_ms)
        _open_save_dialog(page, lat, lon)
        try:
            if _find_list_item(page, name) is not None:
                return "created"
        finally:
            _close_dialog(page)
    raise OpFailed(f"list '{name}' not visible 21 s after Create; the next run adopts it if Google created it")


def add_point(page, name: str, lat: float, lon: float) -> str:
    """Tick our list for the place at (lat, lon) and verify after a reload. Returns "added" or "already"."""
    _open_save_dialog(page, lat, lon)
    row = _require_list_item(page, name)
    if _is_checked(page, row):
        _close_dialog(page)
        return "already"
    row.click(timeout=10000)
    _close_dialog(page)
    _verify(page, name, lat, lon, expected=True)
    return "added"


def remove_point(page, name: str, lat: float, lon: float) -> str:
    """Untick our list for the place at (lat, lon) and verify after a reload. Returns "removed" or "absent"."""
    _open_save_dialog(page, lat, lon)
    row = _require_list_item(page, name)
    if not _is_checked(page, row):
        _close_dialog(page)
        return "absent"
    row.click(timeout=10000)
    _close_dialog(page)
    _verify(page, name, lat, lon, expected=False)
    return "removed"


_COUNT_RE = re.compile(r"(\d[\d,]*)\s*(?:places?|個地點|個地方)", re.IGNORECASE)


def parse_place_count(text: str) -> Optional[int]:
    """'Private · 220 places' -> 220; None when no count is in the text."""
    m = _COUNT_RE.search(text or "")
    return int(m.group(1).replace(",", "")) if m else None


def count_list(page, name: str, lat: float, lon: float) -> int:
    """
    Read the place count Google shows for our list. The Save menu of any place page already carries
    it ("Private · 220 places" in the list's row), so this opens that menu, reads the one row and
    closes the menu. It ticks nothing and writes nothing.
    """
    _open_save_dialog(page, lat, lon)
    try:
        n = parse_place_count(_require_list_item(page, name).inner_text())
        if n is None:
            _fail(page, f"no place count in the row for '{name}' (LIST_ITEM)")
    finally:
        _close_dialog(page)
    return n


class PageOps:
    """
    The ops bound to one Playwright page; this is what run_sync drives. Any browser error
    (timeout, closed target, ...) is first checked against the page: a sign-in, consent or
    captcha page means exit 3, not a selector failure.
    """

    def __init__(self, page):
        self.page = page

    def _guarded(self, fn, *args):
        try:
            return fn(self.page, *args)
        except (Blocked, ReloginNeeded):
            raise
        except Exception:
            try:
                check_blocked(self.page)
            except (Blocked, ReloginNeeded):
                raise
            except Exception:
                pass
            raise

    def check_session(self) -> None:
        self._guarded(check_session)

    def ensure_list(self, task: Task) -> str:
        return self._guarded(ensure_list, task.list, task.point["lat"], task.point["lon"])

    def add(self, task: Task) -> str:
        return self._guarded(add_point, task.list, task.point["lat"], task.point["lon"])

    def remove(self, task: Task) -> str:
        return self._guarded(remove_point, task.list, task.point["lat"], task.point["lon"])

    def count(self, name: str, lat: float, lon: float) -> int:
        return self._guarded(count_list, name, lat, lon)

    def failure_screenshot(self, path: Path) -> None:
        with contextlib.suppress(Exception):
            self.page.screenshot(path=str(path))


# ---- the run loop ----------------------------------------------------------------------
def _point_record(point: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "lat": point["lat"],
        "lon": point["lon"],
        "road": point.get("road", ""),
        "payload_hash": point.get("payload_hash", ""),
        "synced_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def _apply_to_state(state: Dict[str, Any], task: Task) -> None:
    entry = state.setdefault("lists", {}).setdefault(task.list, {})
    entry.setdefault("points", {})
    if task.op == "ensure":
        entry["exists"] = True
    elif task.op == "add":
        entry["exists"] = True
        for p in task.points:
            entry["points"][p["point_id"]] = _point_record(p)
    elif task.op in ("remove", "forget"):
        for p in task.points:
            entry["points"].pop(p["point_id"], None)


def run_sync(
    *,
    lists_dir: str,
    state_path: Path,
    ops_factory: Callable[[], Any],
    log: RunLog,
    only: Optional[List[str]] = None,
    max_ops: int = DEFAULT_MAX_OPS,
    rng: Optional[random.Random] = None,
    shot_dir: Optional[Path] = None,
) -> int:
    """
    Apply the plan through ops_factory() (a context manager yielding the ops
    object; it is only entered when a Google op is needed, so an up-to-date run
    never starts a browser). Returns the exit code.

    State is rewritten atomically after every verified op and never otherwise,
    so a crash or a stop loses at most the op in flight, and the next run plans
    that op again (add and remove are idempotent). Lists outside index.json are
    never planned, opened or written. --max-ops counts every Google op, list
    creation included.

    sync.log has: a "start" line, per op an "intent" line (before dispatch) and an "op"
    line (outcome and reason), "skipped" lines for entities not processed, a
    "state_write_failed" line if the state cannot be saved, and a "summary" line.
    """
    rng = rng or random.Random()
    state_path = Path(state_path)
    state = load_state(str(state_path))
    built = build_tasks(lists_dir, state, only)
    tasks: List[Task] = built["tasks"]
    allowed = set(load_index_names(lists_dir))
    google_ops = [t for t in tasks if t.op in ("add", "remove")]
    planned = len(google_ops)
    counters = {"done": 0, "failed": 0, "ids_done": 0}
    ids_planned = sum(len(t.points) for t in tasks if t.op != "ensure")
    log.event(event="start", planned=planned, ids_planned=ids_planned, lists=built["lists"],
              adds=built["adds"], removes=built["removes"], places_to_add=built["places_to_add"],
              groups_multi=built["groups_multi"], max_ops=max_ops, state=str(state_path),
              ignored_state_lists=built["ignored_state_lists"])
    for s in built["skipped"]:
        log.event(event="skipped", **s)

    def finish(code: int, reason: str = "") -> int:
        log.event(event="summary", planned=planned, done=counters["done"], failed=counters["failed"],
                  remaining=planned - counters["done"], ids_planned=ids_planned,
                  ids_done=counters["ids_done"], exit=code, reason=reason)
        return code

    def settle(task: Task, result: str, t0: float, reason: str = "") -> bool:
        """Record a verified outcome in state and log it. False = state could not be written."""
        _apply_to_state(state, task)
        try:
            write_state_atomic(state_path, state)
        except Exception as e:
            log.event(event="op", list=task.list, place=task.key, point_id=task.point_id, op=task.op,
                      result="ok_state_not_saved", ms=int((time.monotonic() - t0) * 1000),
                      reason=f"verified in Google but state write failed: {type(e).__name__}: {e}")
            log.event(event="state_write_failed", list=task.list, place=task.key, ids=task.ids,
                      reason=f"{type(e).__name__}: {e}", path=str(state_path))
            return False
        if task.op in ("add", "remove"):
            counters["done"] += 1
        if task.op != "ensure":
            counters["ids_done"] += len(task.points)
        log.event(event="op", list=task.list, place=task.key, point_id=task.point_id, op=task.op,
                  result=result, ms=int((time.monotonic() - t0) * 1000), reason=reason)
        return True

    if not tasks:
        return finish(EXIT_OK, "nothing to do")

    # state-only work (ids that left the plan while their place is still wanted) needs no browser
    for task in [t for t in tasks if t.op == "forget"]:
        assert task.list in allowed
        log.event(event="intent", list=task.list, place=task.key, op="forget", ids=task.ids)
        if not settle(task, "state_only", time.monotonic(), "place still wanted by another id; no Google op"):
            return finish(EXIT_ERROR, "state write failed")
    tasks = [t for t in tasks if t.op != "forget"]
    if not tasks:
        return finish(EXIT_OK, "state updated, no Google op needed")

    with ops_factory() as ops:
        t0 = time.monotonic()
        try:
            ops.check_session()
        except SyncError as e:
            reason = "blocked" if isinstance(e, Blocked) else "relogin needed"
            log.event(event="op", list=None, point_id=None, op="session", result=reason.replace(" ", "_"),
                      ms=int((time.monotonic() - t0) * 1000), reason=str(e))
            return finish(EXIT_RELOGIN, reason)
        log.event(event="op", list=None, point_id=None, op="session", result="ok",
                  ms=int((time.monotonic() - t0) * 1000), reason="")

        executed = 0          # Google ops run so far (pacing)
        consecutive = 0
        shots = 0
        skip_lists = set()
        for task in tasks:
            if task.list in skip_lists:
                log.event(event="skipped", list=task.list, place=task.key, op=task.op, ids=task.ids,
                          reason="list could not be ensured earlier in this run")
                continue
            assert task.list in allowed, f"refusing to touch list outside index.json: {task.list}"
            if executed >= max_ops:
                log.event(event="skipped", list=task.list, place=task.key, op=task.op, ids=task.ids,
                          reason=f"max-ops {max_ops} reached; first skipped task, the rest follow next run")
                return finish(EXIT_PARTIAL, "max-ops reached")
            if executed:
                time.sleep(rng.uniform(*PACE_SECONDS))
                if executed % LONG_PAUSE_EVERY == 0:
                    time.sleep(LONG_PAUSE_SECONDS)
            executed += 1

            log.event(event="intent", list=task.list, place=task.key, op=task.op, ids=task.ids or [task.point_id])
            t0 = time.monotonic()
            try:
                if task.op == "ensure":
                    result = ops.ensure_list(task)
                elif task.op == "add":
                    result = ops.add(task)
                else:
                    result = ops.remove(task)
            except (Blocked, ReloginNeeded) as e:
                reason = "blocked" if isinstance(e, Blocked) else "relogin needed"
                counters["failed"] += 1
                log.event(event="op", list=task.list, place=task.key, point_id=task.point_id, op=task.op,
                          result="fail", ms=int((time.monotonic() - t0) * 1000), reason=f"{reason}: {e}")
                return finish(EXIT_RELOGIN, reason)
            except Exception as e:   # OpFailed, playwright timeouts, anything else in one op
                counters["failed"] += 1
                consecutive += 1
                log.event(event="op", list=task.list, place=task.key, point_id=task.point_id, op=task.op,
                          result="fail", ms=int((time.monotonic() - t0) * 1000),
                          reason=f"{type(e).__name__}: {e}")
                if shot_dir and shots < MAX_FAILURE_SHOTS and hasattr(ops, "failure_screenshot"):
                    shots += 1
                    ops.failure_screenshot(Path(shot_dir) / f"{log.run_id}-fail{shots}.png")
                if task.op == "ensure":
                    skip_lists.add(task.list)   # nothing to add to a list that could not be ensured
                if consecutive >= MAX_CONSECUTIVE_FAILURES:
                    return finish(EXIT_FAILURES, f"{consecutive} consecutive failures")
                continue

            consecutive = 0
            if not settle(task, result, t0):
                return finish(EXIT_ERROR, "state write failed")

    if counters["done"] < planned:
        return finish(EXIT_PARTIAL, "some ops failed or were skipped; they are planned again next run")
    return finish(EXIT_OK)


# ---- CLI entry -------------------------------------------------------------------------
def launch_kwargs(profile: str, headed: bool = True) -> Dict[str, Any]:
    """Arguments for chromium.launch_persistent_context. No credentials, ever."""
    return {
        "user_data_dir": str(Path(profile).expanduser()),
        "channel": "chrome",
        "headless": not headed,
        "args": list(LAUNCH_ARGS),
        "viewport": dict(VIEWPORT),
    }


@contextlib.contextmanager
def chrome_ops(profile: str, headed: bool = True):
    """Start Chrome on the logged-in profile and yield PageOps; Chrome is closed on exit."""
    from playwright.sync_api import sync_playwright   # imported here: dry-run and tests need no playwright

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(**launch_kwargs(profile, headed))
        try:
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.set_default_timeout(15000)
            yield PageOps(page)
        finally:
            with contextlib.suppress(Exception):
                ctx.close()


def sync_lists(
    *,
    lists_dir: str = "data/lists",
    state: str = DEFAULT_STATE,
    profile: str = DEFAULT_PROFILE,
    only: Optional[List[str]] = None,
    max_ops: int = DEFAULT_MAX_OPS,
    dry_run: bool = False,
    headed: bool = True,
    count: Optional[str] = None,
    ops_factory: Optional[Callable[[], Any]] = None,
    log_path: Optional[str] = None,
    run_id: Optional[str] = None,
) -> int:
    """CLI behind `main.py sync-lists`; returns the exit code."""
    state_path = Path(state).expanduser()
    log = RunLog(Path(log_path).expanduser() if log_path else state_path.parent / "sync.log",
                 run_id=run_id or uuid.uuid4().hex[:8])
    factory = ops_factory or (lambda: chrome_ops(profile, headed))
    try:
        if count is not None:
            return _count_cmd(lists_dir, state_path, count, factory, log)
        if dry_run:
            built = build_tasks(lists_dir, load_state(str(state_path)), only)
            google_ops = sum(1 for t in built["tasks"] if t.op in ("add", "remove", "ensure"))
            print(f"sync-lists dry-run: {built['lists']} lists / {built['adds']} adds / "
                  f"{built['removes']} removes ({built['unchanged']} unchanged)  [counts are point ids]")
            print(f"  Google places: {built['places_to_add']} to add; "
                  f"{built['groups_multi']} place groups hold more than one id ({built['extra_ids']} extra ids), "
                  f"so --count will show places, not ids")
            print(f"  this run would execute {min(google_ops, max_ops)} of {google_ops} Google ops "
                  f"(list creation included; --max-ops {max_ops}); state: {state_path}")
            if built["ignored_state_lists"]:
                print(f"  state lists not in index.json (never touched): {built['ignored_state_lists']}")
            return EXIT_OK
        return run_sync(lists_dir=lists_dir, state_path=state_path, ops_factory=factory, log=log,
                        only=only, max_ops=max_ops, shot_dir=state_path.parent / "fail")
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        log.event(event="summary", exit=EXIT_ERROR, reason=str(e))
        return EXIT_ERROR
    except Exception as e:
        log.event(event="summary", exit=EXIT_ERROR, reason=f"{type(e).__name__}: {e}")
        print(f"sync-lists failed: {type(e).__name__}: {e}", file=sys.stderr)
        return EXIT_ERROR


def state_counts(state: Dict[str, Any], name: str) -> Dict[str, int]:
    """Points (ids) and distinct Google places recorded for one list."""
    points = state.get("lists", {}).get(name, {}).get("points", {})
    places = {place_key(v["lat"], v["lon"]) for v in points.values()
              if v.get("lat") is not None and v.get("lon") is not None}
    return {"points": len(points), "places": len(places)}


def _count_cmd(lists_dir: str, state_path: Path, name: str, factory, log: RunLog) -> int:
    if name not in load_index_names(lists_dir):
        raise ValueError(f"not a list in {lists_dir}/index.json: {name!r}")
    first = first_desired_point(lists_dir, name)
    if first is None:
        raise ValueError(f"list {name!r} has no points in {lists_dir}, so there is no place page to read its count from")
    mine = state_counts(load_state(str(state_path)), name)
    log.event(event="start", op="count", list=name, state=str(state_path), **{f"state_{k}": v for k, v in mine.items()})
    with factory() as ops:
        t0 = time.monotonic()
        try:
            ops.check_session()
            shown = ops.count(name, *first)
        except SyncError as e:
            ms_ = int((time.monotonic() - t0) * 1000)
            if isinstance(e, (Blocked, ReloginNeeded)):
                reason = "blocked" if isinstance(e, Blocked) else "relogin needed"
                log.event(event="op", list=name, op="count", result="fail", ms=ms_, reason=str(e))
                log.event(event="summary", exit=EXIT_RELOGIN, reason=reason)
                return EXIT_RELOGIN
            log.event(event="op", list=name, op="count", result="fail", ms=ms_, reason=str(e))
            log.event(event="summary", exit=EXIT_ERROR, reason=str(e))
            print(f"count failed: {e}", file=sys.stderr)
            return EXIT_ERROR
    log.event(event="op", list=name, op="count", result="ok", ms=int((time.monotonic() - t0) * 1000),
              reason=f"google={shown} state_places={mine['places']} state_points={mine['points']}")
    log.event(event="summary", exit=EXIT_OK, reason="count")
    print(f"list: {name}\ngoogle shows: {shown} places\nstate has:    {mine['places']} places "
          f"({mine['points']} points)")
    return EXIT_OK
