"""Google Maps list sync: plan, state, run loop, exit codes, ops over a fake page. No network, no browser."""

import contextlib
import json
import random
import re
from pathlib import Path

import pytest

import scripts.lists.maps_sync as ms
import scripts.main as main_mod
from scripts.google_maps.selectors import GoogleMapsSelectors as S
from scripts.lists.sync_plan import payload_hash, point_id

REPO = Path(__file__).resolve().parent.parent
LIST_A = "臺北市 中山區"
LIST_B = "臺北市 大安區"
OLD_BIG_LIST = "Taiwan Handicapped Parking"

COLS = "point_id,city,district,source_area,road,dd_lat,dd_long,dms_lat,dms_long"


# ---- fixtures ---------------------------------------------------------------------------
def make_lists(tmp_path, lists):
    """lists: {name: [(lat, lon, road), ...]} -> data/lists style directory."""
    d = tmp_path / "lists"
    d.mkdir()
    index = []
    for n, (name, pts) in enumerate(lists.items()):
        fname = f"list{n}.csv"
        lines = [COLS]
        for lat, lon, road, *suffix in pts:
            pid = point_id("Taipei City", lat, lon) + (suffix[0] if suffix else "")
            lines.append(f'"{pid}",Taipei City,x,x,{road},{lat},{lon},,')
        (d / fname).write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
        index.append({"list": name, "file": fname, "city": "Taipei City", "district": "x",
                      "part": None, "rows": len(pts)})
    (d / "index.json").write_text(json.dumps({"lists": index, "total_rows": 0}, ensure_ascii=False))
    return str(d)


def pts(n, base=25.0):
    return [(round(base + i * 0.001, 6), 121.5, f"road{i}") for i in range(n)]


def pid(lat, lon=121.5):
    return point_id("Taipei City", lat, lon)


class FakeOps:
    """Stands in for PageOps: scripted per-call outcomes, records every call."""

    def __init__(self, fail_at=(), exc_at=None, session_exc=None, crash_at=None):
        self.calls = []            # (op, list, point_id) for every attempted op
        self.fail_at = set(fail_at)   # 1-based index among add/remove calls -> OpFailed
        self.exc_at = exc_at          # {index: exception} for add/remove calls
        self.session_exc = session_exc
        self.crash_at = crash_at      # BaseException subclass raised at this add/remove index
        self.n = 0
        self.session_checked = 0

    def check_session(self):
        self.session_checked += 1
        if self.session_exc:
            raise self.session_exc

    def ensure_list(self, task):
        self.calls.append(("ensure", task.list, task.point_id))
        return "created"

    def _op(self, kind, task):
        self.n += 1
        self.calls.append((kind, task.list, task.point_id))
        if self.crash_at and self.n == self.crash_at:
            raise KeyboardInterrupt()
        if self.exc_at and self.n in self.exc_at:
            raise self.exc_at[self.n]
        if self.n in self.fail_at:
            raise ms.OpFailed(f"scripted failure {self.n}")
        return "added" if kind == "add" else "removed"

    def add(self, task):
        return self._op("add", task)

    def remove(self, task):
        return self._op("remove", task)

    def failure_screenshot(self, path):
        pass


def factory(ops):
    return lambda: contextlib.nullcontext(ops)


def never_factory():
    raise AssertionError("browser must not be started")


def run(tmp_path, lists_dir, ops, *, state=None, **kw):
    state = state or tmp_path / "state" / "sync_state.json"
    log = ms.RunLog(tmp_path / "state" / "sync.log", "t1", echo=False)
    code = ms.run_sync(lists_dir=lists_dir, state_path=state, ops_factory=factory(ops), log=log,
                       rng=random.Random(1), **kw)
    return code, Path(state), tmp_path / "state" / "sync.log"


def read_state(p):
    return json.loads(Path(p).read_text(encoding="utf-8"))


def log_lines(p):
    return [json.loads(x) for x in Path(p).read_text(encoding="utf-8").splitlines()]


# ---- plan ----------------------------------------------------------------------------------
def test_empty_state_is_all_adds_plus_one_ensure_per_list(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(3), LIST_B: pts(2, 24.0)})
    built = ms.build_tasks(d, {"version": 1, "lists": {}})
    assert (built["lists"], built["adds"], built["removes"]) == (2, 5, 0)
    ops = [(t.list, t.op) for t in built["tasks"]]
    assert ops == [(LIST_A, "ensure")] + [(LIST_A, "add")] * 3 + [(LIST_B, "ensure")] + [(LIST_B, "add")] * 2


def test_vanished_point_is_a_remove_and_changed_point_is_an_add(tmp_path):
    d = make_lists(tmp_path, {LIST_A: [(25.0, 121.5, "newroad"), (25.1, 121.5, "same")]})
    state = {"version": 1, "lists": {LIST_A: {"exists": True, "points": {
        pid(25.0): {"lat": 25.0, "lon": 121.5, "road": "oldroad", "payload_hash": payload_hash("oldroad", 25.0, 121.5)},
        pid(25.1): {"lat": 25.1, "lon": 121.5, "road": "same", "payload_hash": payload_hash("same", 25.1, 121.5)},
        pid(25.2): {"lat": 25.2, "lon": 121.5, "road": "gone", "payload_hash": "x"},
    }}}}
    built = ms.build_tasks(d, state)
    seq = [(t.op, t.point_id) for t in built["tasks"]]
    # the vanished point's place is unticked; the changed point keeps its place wanted, so it is only
    # re-added (a tick that is already there), never removed first; no ensure (list is recorded)
    assert seq == [("remove", pid(25.2)), ("add", pid(25.0))]
    assert (built["adds"], built["removes"], built["unchanged"]) == (1, 2, 1)   # plan counts are ids


def test_lists_not_in_index_are_never_planned(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(2)})
    state = {"version": 1, "lists": {
        OLD_BIG_LIST: {"exists": True, "points": {"X:1,1": {"lat": 1, "lon": 1, "road": "r", "payload_hash": "h"}}},
        "臺北市 已不存在": {"points": {"X:2,2": {"lat": 2, "lon": 2}}},
    }}
    built = ms.build_tasks(d, state)
    assert {t.list for t in built["tasks"]} == {LIST_A}
    assert built["removes"] == 0
    assert built["ignored_state_lists"] == sorted([OLD_BIG_LIST, "臺北市 已不存在"])


def test_run_never_touches_a_list_outside_the_index(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(2)})
    state_path = tmp_path / "state" / "sync_state.json"
    ms.write_state_atomic(state_path, {"version": 1, "lists": {
        OLD_BIG_LIST: {"exists": True, "points": {"X:1,1": {"lat": 1, "lon": 1, "road": "r", "payload_hash": "h"}}}}})
    ops = FakeOps()
    code, _, _ = run(tmp_path, d, ops, state=state_path)
    assert code == 0
    assert {c[1] for c in ops.calls} == {LIST_A}
    after = read_state(state_path)
    assert after["lists"][OLD_BIG_LIST]["points"] == {"X:1,1": {"lat": 1, "lon": 1, "road": "r", "payload_hash": "h"}}


def test_only_filters_and_unknown_name_is_an_error(tmp_path, capsys):
    d = make_lists(tmp_path, {LIST_A: pts(2), LIST_B: pts(3, 24.0)})
    built = ms.build_tasks(d, {"lists": {}}, only=[LIST_B])
    assert {t.list for t in built["tasks"]} == {LIST_B} and built["adds"] == 3
    with pytest.raises(ValueError):
        ms.build_tasks(d, {"lists": {}}, only=["not a list"])
    code = ms.sync_lists(lists_dir=d, state=str(tmp_path / "s.json"), only=["not a list"],
                         dry_run=True, ops_factory=never_factory)
    assert code == 1


def test_dry_run_on_repo_data_counts_and_touches_nothing(tmp_path, capsys):
    state = tmp_path / "state" / "sync_state.json"
    code = ms.sync_lists(lists_dir=str(REPO / "data" / "lists"), state=str(state), dry_run=True,
                         ops_factory=never_factory)
    out = capsys.readouterr().out
    assert code == 0
    assert "33 lists / 2609 adds / 0 removes" in out
    assert not state.parent.exists()          # no state, no log, no directory
    assert main_mod.DEFAULT_STATE != "data/sync_state.json"


def test_default_state_and_profile_are_outside_the_repo():
    assert ms.DEFAULT_STATE == "~/.local/state/thp-maps/sync_state.json"
    assert ms.DEFAULT_PROFILE == "~/.config/thp-maps-profile"


# ---- state ----------------------------------------------------------------------------------
def test_state_write_is_atomic_when_replace_crashes(tmp_path, monkeypatch):
    p = tmp_path / "sync_state.json"
    ms.write_state_atomic(p, {"version": 1, "lists": {"a": {"points": {}}}})
    before = p.read_text(encoding="utf-8")

    def boom(*_a, **_k):
        raise OSError("disk gone")

    monkeypatch.setattr(ms.os, "replace", boom)
    with pytest.raises(OSError):
        ms.write_state_atomic(p, {"version": 1, "lists": {"b": {"points": {}}}})
    assert p.read_text(encoding="utf-8") == before          # old file intact and valid
    assert list(tmp_path.glob("*.tmp-*")) == []             # no tmp litter


def test_crash_mid_run_keeps_verified_ops_and_resume_does_not_repeat_them(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(6)})
    crash = FakeOps(crash_at=4)
    with pytest.raises(KeyboardInterrupt):
        run(tmp_path, d, crash)
    state_path = tmp_path / "state" / "sync_state.json"
    st = read_state(state_path)
    assert len(st["lists"][LIST_A]["points"]) == 3          # ops 1-3 verified, op 4 in flight lost
    assert st["lists"][LIST_A]["exists"] is True

    resume = FakeOps()
    code, _, _ = run(tmp_path, d, resume)
    assert code == 0
    assert [c[0] for c in resume.calls] == ["add"] * 3      # no ensure again, only the 3 missing points
    done_ids = {c[2] for c in crash.calls[1:4]}
    assert done_ids.isdisjoint({c[2] for c in resume.calls})
    assert len(read_state(state_path)["lists"][LIST_A]["points"]) == 6


def test_changed_point_failed_add_keeps_old_record_and_retries(tmp_path):
    d = make_lists(tmp_path, {LIST_A: [(25.0, 121.5, "newroad")]})
    state_path = tmp_path / "state" / "sync_state.json"
    old = {"lat": 25.0, "lon": 121.5, "road": "old", "payload_hash": payload_hash("old", 25.0, 121.5)}
    ms.write_state_atomic(state_path, {"version": 1, "lists": {LIST_A: {"exists": True, "points": {pid(25.0): old}}}})
    ops = FakeOps(fail_at={1})
    code, _, _ = run(tmp_path, d, ops, state=state_path)
    assert code == 2 and [c[0] for c in ops.calls] == ["add"]            # no remove first
    assert read_state(state_path)["lists"][LIST_A]["points"][pid(25.0)] == old
    ops2 = FakeOps()
    assert run(tmp_path, d, ops2, state=state_path)[0] == 0
    rec = read_state(state_path)["lists"][LIST_A]["points"][pid(25.0)]
    assert rec["road"] == "newroad" and rec["payload_hash"] == payload_hash("newroad", 25.0, 121.5)


def test_state_points_record_what_the_plan_needs(tmp_path):
    d = make_lists(tmp_path, {LIST_A: [(25.0, 121.5, "roadX")]})
    code, sp, _ = run(tmp_path, d, FakeOps())
    rec = read_state(sp)["lists"][LIST_A]["points"][pid(25.0)]
    assert rec["lat"] == 25.0 and rec["lon"] == 121.5 and rec["road"] == "roadX"
    assert rec["payload_hash"] == payload_hash("roadX", 25.0, 121.5)
    # a second run plans nothing
    ops = FakeOps()
    assert run(tmp_path, d, ops)[0] == 0 and ops.calls == []


# ---- run loop: exit codes -------------------------------------------------------------------
def test_nothing_to_do_exits_0_without_starting_a_browser(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(1)})
    run(tmp_path, d, FakeOps())
    log = ms.RunLog(tmp_path / "state" / "sync.log", "t2", echo=False)
    code = ms.run_sync(lists_dir=d, state_path=tmp_path / "state" / "sync_state.json",
                       ops_factory=never_factory, log=log)
    assert code == 0
    assert log_lines(tmp_path / "state" / "sync.log")[-1]["reason"] == "nothing to do"


def test_max_ops_gives_exit_2_and_next_run_continues(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(5)})
    ops = FakeOps()
    code, sp, logp = run(tmp_path, d, ops, max_ops=3)
    assert code == 2
    assert [c[0] for c in ops.calls] == ["ensure", "add", "add"]    # ensure is one of the 3 ops
    summary = log_lines(logp)[-1]
    assert (summary["planned"], summary["done"], summary["remaining"], summary["exit"]) == (5, 2, 3, 2)
    ops2 = FakeOps()
    code2, _, _ = run(tmp_path, d, ops2, max_ops=3)
    assert code2 == 0 and [c[0] for c in ops2.calls] == ["add", "add", "add"]    # exactly the cap, nothing left
    assert run(tmp_path, d, FakeOps(), max_ops=3)[0] == 0
    assert len(read_state(sp)["lists"][LIST_A]["points"]) == 5


def test_max_ops_exactly_the_work_is_exit_0(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(3)})
    assert run(tmp_path, d, FakeOps(), max_ops=4)[0] == 0          # 1 ensure + 3 adds


def test_three_consecutive_failures_exit_4_and_state_keeps_only_verified(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(8)})
    ops = FakeOps(fail_at={3, 4, 5})
    code, sp, logp = run(tmp_path, d, ops)
    assert code == 4
    assert len([c for c in ops.calls if c[0] == "add"]) == 5        # stopped right after the 3rd failure
    points = read_state(sp)["lists"][LIST_A]["points"]
    verified = {c[2] for i, c in enumerate([c for c in ops.calls if c[0] == "add"], start=1) if i not in {3, 4, 5}}
    assert set(points) == verified and len(points) == 2
    s = log_lines(logp)[-1]
    assert s["exit"] == 4 and s["failed"] == 3 and s["done"] == 2 and s["remaining"] == 6


def test_failures_that_are_not_consecutive_do_not_stop_the_run(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(8)})
    ops = FakeOps(fail_at={2, 4, 6})
    code, sp, logp = run(tmp_path, d, ops)
    assert code == 2                                               # finished its pass, 3 ops left for tomorrow
    assert len(read_state(sp)["lists"][LIST_A]["points"]) == 5
    assert log_lines(logp)[-1]["remaining"] == 3
    # next run retries exactly the 3 failed ones
    ops2 = FakeOps()
    assert run(tmp_path, d, ops2)[0] == 0 and len(ops2.calls) == 3


def test_unexpected_exception_in_an_op_counts_as_a_failure(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(4)})
    ops = FakeOps(exc_at={1: TimeoutError("playwright timeout"), 2: RuntimeError("x"), 3: RuntimeError("y")})
    assert run(tmp_path, d, ops)[0] == 4


def test_failed_ensure_skips_that_list_only(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(3), LIST_B: pts(2, 24.0)})

    class Ops(FakeOps):
        def ensure_list(self, task):
            self.calls.append(("ensure", task.list, task.point_id))
            if task.list == LIST_A:
                raise ms.OpFailed("no new list button")
            return "created"

    ops = Ops()
    code, sp, _ = run(tmp_path, d, ops)
    assert code == 2
    assert {c[1] for c in ops.calls if c[0] == "add"} == {LIST_B}
    assert LIST_A not in read_state(sp)["lists"]


def test_signed_out_at_start_exits_3_and_writes_nothing(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(3)})
    ops = FakeOps(session_exc=ms.ReloginNeeded("relogin needed: sign-in link on page"))
    code, sp, logp = run(tmp_path, d, ops)
    assert code == 3
    assert not sp.exists()
    assert ops.calls == []
    last = log_lines(logp)[-1]
    assert last["exit"] == 3 and last["reason"] == "relogin needed"
    assert any(l.get("op") == "session" and l["result"] == "relogin_needed" for l in log_lines(logp))


def test_blocked_at_start_exits_3_reason_blocked(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(3)})
    code, sp, logp = run(tmp_path, d, FakeOps(session_exc=ms.Blocked("blocked: captcha on page")))
    assert code == 3 and not sp.exists()
    assert log_lines(logp)[-1]["reason"] == "blocked"


def test_blocked_mid_run_exits_3_and_keeps_already_verified_ops(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(5)})
    ops = FakeOps(exc_at={3: ms.Blocked("blocked: page says 'unusual traffic'")})
    code, sp, logp = run(tmp_path, d, ops)
    assert code == 3
    assert len(read_state(sp)["lists"][LIST_A]["points"]) == 2
    assert len([c for c in ops.calls if c[0] == "add"]) == 3         # stopped at once, no retries
    assert log_lines(logp)[-1]["reason"] == "blocked"


def test_pacing_6_to_12_seconds_and_30_every_50_ops(tmp_path, monkeypatch):
    sleeps = []
    monkeypatch.setattr(ms.time, "sleep", lambda s: sleeps.append(s))
    d = make_lists(tmp_path, {LIST_A: pts(101)})
    assert run(tmp_path, d, FakeOps())[0] == 0
    short = [s for s in sleeps if s != ms.LONG_PAUSE_SECONDS]
    assert len(short) == 101                    # 102 executed (ensure + 101 adds), none before the first
    assert all(6.0 <= s <= 12.0 for s in short)
    assert sleeps.count(ms.LONG_PAUSE_SECONDS) == 2


def test_one_json_line_per_op_with_the_required_fields(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(2)})
    run(tmp_path, d, FakeOps(fail_at={2}))
    lines = log_lines(tmp_path / "state" / "sync.log")
    ops = [l for l in lines if l.get("event") == "op" and l["op"] in ("add", "remove", "ensure")]
    assert len(ops) == 3
    for l in ops:
        assert set(l) >= {"ts", "run_id", "list", "point_id", "op", "result", "ms", "reason"}
    assert [l["result"] for l in ops] == ["created", "added", "fail"]
    assert "scripted failure" in ops[2]["reason"]
    s = lines[-1]
    assert s["event"] == "summary" and set(s) >= {"planned", "done", "failed", "remaining", "exit"}


# ---- ops over a fake page ---------------------------------------------------------------------
# The fake page mirrors the structure probed on the live site (English UI, hl=en): the Save button is
# button[data-value="Save"|"Saved"]; it opens ONE div[role=menu] whose rows are div[role=menuitemradio]
# with aria-checked, icon-glyph lines, the name and a "Private/Shared · N places" line, plus a " New list"
# button; New list opens a role=dialog form with an unlabelled text input and button[data-value=Create].
# Selectors are matched for real (tag, [attr=v], [attr^=v], [attr*=v], :has-text, "[role=dialog] " ancestor),
# so a test fails when the constant in selectors.py no longer fits this structure.
GLYPH_TICK = ""
GLYPH_LIST = ""


class El:
    def __init__(self, tag, attrs=None, text="", parent=None, kind=None, lines=None):
        self.tag, self.attrs, self.parent, self.kind = tag, dict(attrs or {}), parent, kind
        self.lines = list(lines) if lines is not None else None
        self.text = text
        self.uid = None

    def inner(self):
        return "\n".join(self.lines) if self.lines is not None else self.text

    def ancestors(self):
        e = self.parent
        while e is not None:
            yield e
            e = e.parent


_ATTR = re.compile(r'\[([\w-]+)(\^=|\*=|=)"([^"]*)"\]')
_HAS_TEXT = re.compile(r':has-text\("([^"]*)"\)')
_CLAUSE = re.compile(r'^(?:\[role="dialog"\] )?([a-z]*)(?:#([\w-]+))?((?:\[[^\]]+\]|:has-text\("[^"]*"\))*)$')


def _clause_matches(el, clause):
    m = _CLAUSE.match(clause)
    assert m, f"fake cannot parse selector clause: {clause!r}"
    tag, ident, rest = m.groups()
    if clause.startswith('[role="dialog"] ') and not any(a.attrs.get("role") == "dialog" for a in el.ancestors()):
        return False
    if tag and el.tag != tag:
        return False
    if ident and el.attrs.get("id") != ident:
        return False
    for a, op, v in _ATTR.findall(rest):
        have = el.attrs.get(a)
        if have is None:
            return False
        if op == "=" and have != v or op == "^=" and not have.startswith(v) or op == "*=" and v not in have:
            return False
    for t in _HAS_TEXT.findall(rest):
        if t.lower() not in el.inner().lower():
            return False
    return True


def matches(el, selector):
    return any(_clause_matches(el, c.strip()) for c in selector.split(", "))


class FakeLocator:
    def __init__(self, page, selector, scope=None, has=None, idx=None):
        self.page, self.selector, self.scope, self.has, self.idx = page, selector, scope, has, idx

    def _all(self):
        everything = self.page.elements()            # fresh tree; elements are compared by uid
        els = [e for e in everything if matches(e, self.selector)]
        if self.scope is not None:
            els = [e for e in els if self.scope.uid in [a.uid for a in e.ancestors()]]
        if self.has is not None:
            els = [e for e in els if any(e.uid in [a.uid for a in x.ancestors()] and matches(x, self.has)
                                         for x in everything)]
        return els

    def locator(self, selector, has_text=None):
        outer = self._all()
        assert len(outer) == 1, "fake: chained locator needs exactly one container"
        return FakeLocator(self.page, selector, scope=outer[0])

    def count(self):
        if self.selector in self.page.extra_counts:
            return self.page.extra_counts[self.selector]
        return len(self._all())

    def nth(self, i):
        return FakeLocator(self.page, self.selector, self.scope, self.has, i)

    def _el(self):
        els = self._all()
        return els[self.idx or 0]

    def inner_text(self, timeout=None):
        return self._el().inner()

    def get_attribute(self, name, timeout=None):
        return self._el().attrs.get(name)

    def fill(self, text, timeout=None):
        assert self._el().kind == "name_input"
        self.page.new_name = text

    def click(self, timeout=None):
        p = self.page
        if p.click_error is not None and self.selector == p.click_error[0]:
            p.url = p.click_error[2]
            raise p.click_error[1]
        if self.selector in p.click_to:
            p.url = p.click_to[self.selector]
            return
        p.click_el(self._el())


class FakeKeyboard:
    def __init__(self, page):
        self.page = page

    def press(self, key):
        if key == "Escape":
            self.page.dialog_open = False


class FakePage:
    """Google Maps as probed live: home, a place page with Save button, Save menu, New list form."""

    def __init__(self, lists=(LIST_A, LIST_B, OLD_BIG_LIST, "Favorites"), signed_in=True):
        self.list_names = list(lists)
        self.places = {}            # "lat,lon" -> set of list names
        self.signed_in = signed_in
        self.captcha = False
        self.body_text = "Google Maps"
        self.save_button_count = 1
        self.dialog_count = 1       # how many menus hold the list rows
        self.stray_rows = []        # list-row lookalikes outside the menu
        self.checked_attr = None    # override aria-checked ("<missing>" = attribute absent)
        self.toggle_works = True
        self.redirect_url = None
        self.new_list_goes_to = None   # url the page jumps to when "New list" is clicked
        self.click_error = None        # (selector, exception, url the page shows afterwards)
        self.extra_counts = {}         # selector -> forced match count
        self.click_to = {}             # selector -> url the page shows after clicking it
        self.url = ""
        self.mode = "blank"
        self.dialog_open = False
        self.creating = False
        self.new_name = ""
        self.coords = None
        self.keyboard = FakeKeyboard(self)
        self.visited = []
        self.dialog_lookalike_rows = False   # an unrelated role=dialog that holds menuitemradio rows
        self.row_role = "menuitemradio"
        self.extra_form_inputs = 0           # additional text inputs inside the New list form
        self.create_variant = "both"         # "both" | "data-value" | "aria-label" | "icon-only"

    # --- test conveniences
    @property
    def save_button(self):
        return self.save_button_count > 0

    @save_button.setter
    def save_button(self, v):
        self.save_button_count = 1 if v else 0

    def membership(self, coords):                 # read-only view
        return self.places.get(coords, set())

    # --- the DOM
    def row_lines(self, i, name):
        if i % 3 == 0:
            return ["", "", name, "Shared · 2,538 places"]
        if i % 3 == 1:
            return [name, "Private · 2,278 places"]
        return [GLYPH_TICK if name in self.membership(self.coords) else GLYPH_LIST, name, "Private · 760 places"]

    def row_attrs(self, i, name):
        attrs = {"role": self.row_role, "data-index": str(i), "jsaction": "actionmenu.select"}
        if self.checked_attr is None:
            attrs["aria-checked"] = "true" if name in self.membership(self.coords) else "false"
        elif self.checked_attr != "<missing>":
            attrs["aria-checked"] = self.checked_attr
        return attrs

    def elements(self):
        els = []
        body = El("body", text=self.body_text)
        els.append(body)
        if self.captcha:
            els.append(El("iframe", {"src": "https://www.google.com/recaptcha/api2/anchor"}))
        if self.signed_in and self.mode in ("home", "place"):
            els.append(El("a", {"aria-label": "Google Account: Test (test@example.com)"}))
        if not self.signed_in:
            els.append(El("a", {"aria-label": "Sign in"}))
        if self.mode != "place":
            return self._number(els)
        # unrelated page furniture: two dialogs, a combobox input and textareas outside any dialog
        for k in range(2):
            d = El("div", {"role": "dialog"})
            els.append(d)
            if k == 0 and self.dialog_lookalike_rows:
                els.append(El("div", {"role": "menuitemradio", "aria-checked": "false"}, parent=d,
                              lines=["Anyone with the link", "Private"]))
        els.append(El("input", {"type": "text", "role": "combobox", "id": "searchboxinput"}))
        els.append(El("textarea", {}))
        on_some_list = bool(self.membership(self.coords))
        for _ in range(self.save_button_count):
            v = "Saved" if on_some_list else "Save"
            els.append(El("button", {"data-value": v, "aria-label": v}, text=v, kind="save_button"))
        for name in self.stray_rows:
            els.append(El("div", {"role": "menuitemradio", "aria-checked": "false"}, lines=[name, "Private · 1 place"]))
        if self.dialog_open:
            for _ in range(self.dialog_count):
                menu = El("div", {"role": "menu", "aria-label": "Save in your lists"})
                els.append(menu)
                for i, name in enumerate(self.list_names):
                    els.append(El("div", self.row_attrs(i, name), parent=menu, kind="row", lines=self.row_lines(i, name)))
                els.append(El("button", {"aria-label": ""}, text=GLYPH_LIST + " New list", parent=menu, kind="new_list"))
        if self.creating:
            form = El("div", {"role": "dialog"})
            create_attrs = {"both": {"data-value": "Create", "aria-label": "Create"},
                            "data-value": {"data-value": "Create"},
                            "aria-label": {"aria-label": "Create"},
                            "icon-only": {"data-value": "Create"}}[self.create_variant]
            create_text = "" if self.create_variant == "icon-only" else "Create"
            els += [form,
                    El("input", {"type": "text"}, parent=form, kind="name_input"),
                    *[El("input", {"type": "text"}, parent=form, kind="name_input") for _ in range(self.extra_form_inputs)],
                    El("button", {"aria-label": "Choose icon"}, text="Choose icon", parent=form),
                    El("button", {"aria-label": "Close"}, text="Close", parent=form),
                    El("button", {"aria-label": "Close"}, text="Close", parent=form),
                    El("button", create_attrs, text=create_text, parent=form, kind="create")]
        return self._number(els)

    @staticmethod
    def _number(els):
        for n, e in enumerate(els):
            e.uid = n
        return els

    def click_el(self, el):
        if el.kind == "save_button":
            self.dialog_open = True
        elif el.kind == "row":
            if self.toggle_works:
                self.places.setdefault(self.coords, set()).symmetric_difference_update(
                    {self.list_names[int(el.attrs["data-index"])]})
        elif el.kind == "new_list":
            if self.new_list_goes_to:
                self.url = self.new_list_goes_to
            else:
                self.creating = True
        elif el.kind == "create":
            self.list_names.append(self.new_name)
            self.places.setdefault(self.coords, set()).add(self.new_name)
            self.creating = False
            self.dialog_open = False

    # --- the Playwright surface the ops use
    def goto(self, url, wait_until=None, timeout=None):
        self.visited.append(url)
        self.url = self.redirect_url or url
        self.dialog_open = False
        self.creating = False
        if url == S.HOME_URL:
            self.mode = "home"
        else:
            m = re.search(r"query=([\d.]+),([\d.]+)", url)
            self.mode = "place"
            self.coords = f"{m.group(1)},{m.group(2)}"

    def locator(self, selector, has_text=None, has=None):
        return FakeLocator(self, selector, has=has.selector if has is not None else None)

    def wait_for_timeout(self, ms):
        pass

    def screenshot(self, path=None):
        pass


def test_session_ok_when_account_button_and_no_sign_in_link():
    ms.check_session(FakePage())


def test_session_signed_out_raises_relogin():
    with pytest.raises(ms.ReloginNeeded):
        ms.check_session(FakePage(signed_in=False))


def test_session_redirected_to_login_raises_relogin():
    p = FakePage()
    p.redirect_url = "https://accounts.google.com/v3/signin/identifier"
    with pytest.raises(ms.ReloginNeeded):
        ms.check_session(p)


@pytest.mark.parametrize("how", ["captcha", "sorry", "consent", "text"])
def test_session_captcha_unusual_traffic_consent_raise_blocked(how):
    p = FakePage()
    if how == "captcha":
        p.captcha = True
    elif how == "sorry":
        p.redirect_url = "https://www.google.com/sorry/index?continue=x"
    elif how == "consent":
        p.redirect_url = "https://consent.google.com/m?continue=x"
    else:
        p.body_text = "Our systems have detected unusual traffic from your computer network."
    with pytest.raises(ms.Blocked):
        ms.check_session(p)


def test_signed_out_page_through_the_runner_exits_3_and_writes_no_state(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(2)})
    page = FakePage(signed_in=False)
    code, sp, logp = run(tmp_path, d, ms.PageOps(page))
    assert code == 3 and not sp.exists()
    assert page.visited == [S.HOME_URL]            # nothing but the session check was opened
    assert log_lines(logp)[-1]["reason"] == "relogin needed"


def test_add_point_ticks_only_our_list_and_verifies():
    p = FakePage()
    assert ms.add_point(p, LIST_A, 25.0, 121.5) == "added"
    assert p.places["25.000000,121.500000"] == {LIST_A}      # other lists untouched
    assert ms.add_point(p, LIST_A, 25.0, 121.5) == "already"


def test_add_point_url_has_six_decimals_and_hl_en():
    p = FakePage()
    ms.add_point(p, LIST_A, 25.053873709199138, 121.53157348840881)
    assert p.visited[0] == "https://www.google.com/maps/search/?api=1&query=25.053874,121.531573&hl=en"


def test_remove_point_unticks_only_our_list():
    p = FakePage()
    p.places["25.000000,121.500000"] = {LIST_A, "Favorites", LIST_B}
    assert ms.remove_point(p, LIST_A, 25.0, 121.5) == "removed"
    assert p.places["25.000000,121.500000"] == {"Favorites", LIST_B}
    assert ms.remove_point(p, LIST_A, 25.0, 121.5) == "absent"


def test_list_name_match_is_exact_not_substring():
    p = FakePage(lists=[LIST_A + "-1", "新" + LIST_A])
    with pytest.raises(ms.OpFailed, match="found 0"):
        ms.add_point(p, LIST_A, 25.0, 121.5)
    assert p.places == {}


def test_add_fails_when_the_tick_does_not_stick():
    p = FakePage()
    p.toggle_works = False
    with pytest.raises(ms.OpFailed, match="verify failed"):
        ms.add_point(p, LIST_A, 25.0, 121.5)


def test_add_fails_when_save_button_is_missing():
    p = FakePage()
    p.save_button = False
    with pytest.raises(ms.OpFailed, match="SAVE_BUTTON"):
        ms.add_point(p, LIST_A, 25.0, 121.5)


def test_op_stops_with_blocked_when_google_interrupts_mid_op():
    p = FakePage()
    p.redirect_url = "https://www.google.com/sorry/index"
    with pytest.raises(ms.Blocked):
        ms.add_point(p, LIST_A, 25.0, 121.5)


def test_ensure_list_exists_changes_nothing():
    p = FakePage()
    assert ms.ensure_list(p, LIST_A, 25.0, 121.5) == "exists"
    assert p.places == {} and p.list_names.count(LIST_A) == 1


def test_ensure_list_creates_the_missing_list_with_the_exact_name():
    p = FakePage(lists=[OLD_BIG_LIST, "Favorites"])
    assert ms.ensure_list(p, LIST_A, 25.0, 121.5) == "created"
    assert p.list_names == [OLD_BIG_LIST, "Favorites", LIST_A]
    # the anchor place was saved into the new list; the add that follows sees it ticked
    assert ms.add_point(p, LIST_A, 25.0, 121.5) == "already"


def test_full_run_over_the_fake_page_creates_list_adds_and_records_state(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(3)})
    page = FakePage(lists=[OLD_BIG_LIST])
    code, sp, _ = run(tmp_path, d, ms.PageOps(page))
    assert code == 0
    assert page.list_names == [OLD_BIG_LIST, LIST_A]
    assert len(read_state(sp)["lists"][LIST_A]["points"]) == 3
    assert all(v == {LIST_A} for v in page.places.values()) and len(page.places) == 3


def test_parse_place_count():
    assert ms.parse_place_count("臺北市 中山區\nPrivate · 220 places") == 220
    assert ms.parse_place_count("Private · 1 place") == 1
    assert ms.parse_place_count("Shared · 1,234 places") == 1234
    assert ms.parse_place_count("私人 · 220 個地點") == 220
    assert ms.parse_place_count("nothing here") is None


def test_count_command_prints_google_and_state_numbers_and_writes_nothing(tmp_path, capsys):
    d = make_lists(tmp_path, {LIST_A: pts(3)})
    code, sp, _ = run(tmp_path, d, FakeOps())
    before = sp.read_text(encoding="utf-8")

    class CountOps(FakeOps):
        def count(self, name):
            return 2

    code = ms.sync_lists(lists_dir=d, state=str(sp), count=LIST_A, ops_factory=factory(CountOps()))
    out = capsys.readouterr().out
    assert code == 0
    assert "google shows: 2 places" in out and "state has:    3 places (3 points)" in out
    assert sp.read_text(encoding="utf-8") == before


def test_count_signed_out_exits_3(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(1)})
    ops = FakeOps(session_exc=ms.ReloginNeeded("x"))
    assert ms.sync_lists(lists_dir=d, state=str(tmp_path / "s.json"), count=LIST_A, ops_factory=factory(ops)) == 3


# ---- launch, no credentials ---------------------------------------------------------------------
def test_launch_uses_the_chrome_profile_and_no_credentials():
    kw = ms.launch_kwargs("~/.config/thp-maps-profile", headed=True)
    assert kw["channel"] == "chrome" and kw["headless"] is False
    assert kw["args"] == ["--password-store=basic"]
    assert kw["user_data_dir"] == str(Path("~/.config/thp-maps-profile").expanduser())
    assert ms.launch_kwargs("p", headed=False)["headless"] is True
    src = (REPO / "scripts" / "lists" / "maps_sync.py").read_text(encoding="utf-8")
    assert not re.search(r"GOOGLE_(EMAIL|PASSWORD)|getenv|environ", src)
    sel = (REPO / "scripts" / "google_maps" / "selectors.py").read_text(encoding="utf-8")
    assert 'type="password"' not in sel and 'type="email"' not in sel


# ---- CLI + retirement + workflow ------------------------------------------------------------------
def test_cli_sync_maps_and_authenticate_are_retired_and_exit_1(monkeypatch, capsys):
    for cmd in ("sync-maps", "authenticate"):
        monkeypatch.setattr("sys.argv", ["main.py", cmd])
        with pytest.raises(SystemExit) as e:
            main_mod.main()
        assert e.value.code == 1
    assert "retired" in capsys.readouterr().err


def test_cli_sync_lists_dry_run_wires_the_flags(monkeypatch, tmp_path):
    seen = {}

    def fake(**kw):
        seen.update(kw)
        return 0

    monkeypatch.setattr(main_mod, "sync_lists", fake)
    monkeypatch.setattr("sys.argv", ["main.py", "sync-lists", "--dry-run", "--only", "a", "--only", "b",
                                     "--max-ops", "7", "--count", "c", "--profile", "/p", "--state", "/s.json",
                                     "--no-headed"])
    with pytest.raises(SystemExit) as e:
        main_mod.main()
    assert e.value.code == 0
    assert seen == {"lists_dir": "data/lists", "state": "/s.json", "profile": "/p", "only": ["a", "b"],
                    "max_ops": 7, "dry_run": True, "headed": False, "count": "c", "run_id": None}
    seen.clear()
    monkeypatch.setattr("sys.argv", ["main.py", "sync-lists"])
    with pytest.raises(SystemExit):
        main_mod.main()
    assert seen["state"] == ms.DEFAULT_STATE and seen["headed"] is True and seen["max_ops"] == 300
    assert seen["only"] is None and seen["dry_run"] is False and seen["count"] is None and seen["run_id"] is None


def test_workflow_has_no_google_credentials_or_sync_job():
    text = (REPO / ".github" / "workflows" / "update_parking_data.yml").read_text(encoding="utf-8")
    for needle in ("GOOGLE_PASSWORD", "GOOGLE_EMAIL", "google_auth_state", "sync-google-maps",
                   "sync-maps", "authenticate", "playwright"):
        assert needle not in text, needle
    assert "collect-and-update" in text and "scripts/main.py collect" in text and "git push origin main" in text


def test_no_password_flow_modules_or_secrets_left_in_the_repo():
    assert not (REPO / "scripts" / "google_maps" / "authenticator.py").exists()
    assert not (REPO / "scripts" / "google_maps" / "map_saver.py").exists()
    for f in list((REPO / "scripts").rglob("*.py")) + [REPO / "README.md"] + list((REPO / "deploy").rglob("*")):
        if f.is_file() and f.name != "test_maps_sync.py":
            assert "GOOGLE_PASSWORD" not in f.read_text(encoding="utf-8"), f


def test_deploy_files_exist_and_match_the_brief():
    d = REPO / "deploy" / "ubuntu"
    timer = (d / "thp-maps-sync.timer").read_text(encoding="utf-8")
    assert "OnCalendar=*-*-* 10:17:00 Asia/Taipei" in timer and "Persistent=true" in timer
    script = (d / "thp-maps-sync.sh").read_text(encoding="utf-8")
    assert "git -C \"$REPO\" pull --ff-only" in script and "xvfb-run -a" in script
    assert "THP_ALERT_CMD" in script
    service = (d / "thp-maps-sync.service").read_text(encoding="utf-8")
    assert "SuccessExitStatus=2" in service
    assert (d / "README.md").is_file()


# =====================================================================================================
# Codex review loop 1. Each test names the finding it covers (P1..P7).
# =====================================================================================================
import subprocess
import sys
import shutil

SHARED = [(25.0, 121.5, "a", "#aaa"), (25.0, 121.5, "b", "#bbb"), (25.0, 121.5, "c", "#ccc")]
SOLO = (25.5, 121.5, "solo")


def shared_pid(suffix):
    return pid(25.0) + suffix


def google_place(page, lat=25.0, lon=121.5):
    return page.places.get(f"{lat:.6f},{lon:.6f}", set())


# ---- P1: ids that share one Google place --------------------------------------------------------
def test_p1_ids_sharing_a_place_are_one_add_op_and_all_ids_recorded(tmp_path):
    d = make_lists(tmp_path, {LIST_A: SHARED + [SOLO]})
    ops = FakeOps()
    code, sp, logp = run(tmp_path, d, ops)
    assert code == 0
    assert [c[0] for c in ops.calls] == ["ensure", "add", "add"]          # 4 ids, 2 places
    points = read_state(sp)["lists"][LIST_A]["points"]
    assert set(points) == {shared_pid("#aaa"), shared_pid("#bbb"), shared_pid("#ccc"), pid(25.5)}
    s = log_lines(logp)[-1]
    assert (s["planned"], s["done"], s["ids_planned"], s["ids_done"]) == (2, 2, 4, 4)


def test_p1_removing_one_shared_id_keeps_the_google_tick_and_needs_no_browser(tmp_path):
    page = FakePage()
    d = make_lists(tmp_path, {LIST_A: SHARED})
    assert run(tmp_path, d, ms.PageOps(page))[0] == 0
    assert google_place(page) == {LIST_A}
    # the source drops one of the three ids; the place is still wanted by the other two
    (tmp_path / "v2").mkdir()
    d2 = make_lists(tmp_path / "v2", {LIST_A: SHARED[:2]})
    log = ms.RunLog(tmp_path / "state" / "sync.log", "t9", echo=False)
    code = ms.run_sync(lists_dir=d2, state_path=tmp_path / "state" / "sync_state.json",
                       ops_factory=never_factory, log=log)
    assert code == 0
    assert google_place(page) == {LIST_A}                                   # still ticked in Google
    ids = set(read_state(tmp_path / "state" / "sync_state.json")["lists"][LIST_A]["points"])
    assert ids == {shared_pid("#aaa"), shared_pid("#bbb")}                  # state is per id
    forget = [l for l in log_lines(tmp_path / "state" / "sync.log") if l.get("op") == "forget"]
    assert forget and forget[-1]["result"] == "state_only"


def test_p1_google_membership_is_removed_only_when_no_desired_id_is_left(tmp_path):
    page = FakePage()
    d = make_lists(tmp_path, {LIST_A: SHARED + [SOLO]})
    assert run(tmp_path, d, ms.PageOps(page))[0] == 0
    (tmp_path / "v2").mkdir()
    d2 = make_lists(tmp_path / "v2", {LIST_A: [SOLO]})
    code, sp, _ = run(tmp_path, d2, ms.PageOps(page))
    assert code == 0
    assert google_place(page) == set()                                      # nothing wanted there any more
    assert google_place(page, 25.5) == {LIST_A}
    assert set(read_state(sp)["lists"][LIST_A]["points"]) == {pid(25.5)}    # all three ids left state together


def test_p1_remove_group_is_one_op_for_all_its_ids(tmp_path):
    d = make_lists(tmp_path, {LIST_A: [SOLO]})
    state = {"version": 1, "lists": {LIST_A: {"exists": True, "points": {
        shared_pid(x): {"lat": 25.0, "lon": 121.5, "road": x, "payload_hash": "h"} for x in ("#aaa", "#bbb", "#ccc")}}}}
    state["lists"][LIST_A]["points"][pid(25.5)] = {"lat": 25.5, "lon": 121.5, "road": "solo",
                                                   "payload_hash": payload_hash("solo", 25.5, 121.5)}
    built = ms.build_tasks(d, state)
    assert [(t.op, len(t.points)) for t in built["tasks"]] == [("remove", 3)]


def test_p1_changed_id_in_a_shared_place_never_unticks_it(tmp_path):
    page = FakePage()
    d = make_lists(tmp_path, {LIST_A: SHARED})
    assert run(tmp_path, d, ms.PageOps(page))[0] == 0
    sp = tmp_path / "state" / "sync_state.json"
    st = read_state(sp)
    st["lists"][LIST_A]["points"][shared_pid("#bbb")]["payload_hash"] = "old"     # road of one id changed
    ms.write_state_atomic(sp, st)
    page.visited.clear()
    ops = FakeOps()
    code, _, _ = run(tmp_path, d, ops)
    assert code == 0 and [c[0] for c in ops.calls] == ["add"]                  # one add, no remove
    code, _, _ = run(tmp_path, d, ms.PageOps(page))
    assert google_place(page) == {LIST_A}
    assert read_state(sp)["lists"][LIST_A]["points"][shared_pid("#bbb")]["payload_hash"] == payload_hash("b", 25.0, 121.5)


def test_p1_state_write_failure_after_a_verified_tick_resumes_as_already(tmp_path, monkeypatch):
    page = FakePage()
    d = make_lists(tmp_path, {LIST_A: SHARED})
    real = ms.write_state_atomic
    calls = {"n": 0}

    def flaky(path, state):
        calls["n"] += 1
        if calls["n"] == 2:                      # 1st write = ensure, 2nd = the group add
            raise OSError("no space left")
        return real(path, state)

    monkeypatch.setattr(ms, "write_state_atomic", flaky)
    code, sp, logp = run(tmp_path, d, ms.PageOps(page))
    assert code == 1
    assert google_place(page) == {LIST_A}                                      # Google has it, state does not
    assert read_state(sp)["lists"][LIST_A]["points"] == {}
    monkeypatch.setattr(ms, "write_state_atomic", real)
    code2, _, _ = run(tmp_path, d, ms.PageOps(page))
    assert code2 == 0 and len(read_state(sp)["lists"][LIST_A]["points"]) == 3


def test_p1_dry_run_reports_groups_with_more_than_one_id(tmp_path, capsys):
    code = ms.sync_lists(lists_dir=str(REPO / "data" / "lists"), state=str(tmp_path / "s.json"),
                         dry_run=True, ops_factory=never_factory)
    out = capsys.readouterr().out
    assert code == 0
    assert "33 lists / 2609 adds / 0 removes" in out
    assert "2602 to add" in out
    assert "3 place groups hold more than one id (7 extra ids)" in out


def test_p1_state_counts_places_not_ids():
    st = {"lists": {LIST_A: {"points": {
        "a": {"lat": 25.0, "lon": 121.5}, "b": {"lat": 25.0, "lon": 121.5},
        "c": {"lat": 25.1, "lon": 121.5}}}}}
    assert ms.state_counts(st, LIST_A) == {"points": 3, "places": 2}


# ---- P2: the weekly workflow must rebuild and commit the lists ----------------------------------------
def _workflow_steps():
    text = (REPO / ".github" / "workflows" / "update_parking_data.yml").read_text(encoding="utf-8")
    body = text[text.index("    steps:"):]
    return ["- name:" + b for b in body.split("\n      - name:")[1:]]


def _step_index(steps, needle):
    hits = [i for i, st in enumerate(steps) if needle in st]
    assert hits, needle
    return hits[0]


def test_p2_workflow_builds_lists_after_collect_and_before_the_diff_and_commit():
    steps = _workflow_steps()
    collect = _step_index(steps, "python scripts/main.py collect")
    build = _step_index(steps, "python scripts/main.py build-lists")
    check = _step_index(steps, "name: Check for changes")
    commit = _step_index(steps, "name: Create temporary branch and commit")
    assert collect < build < check < commit


def test_p2_build_lists_failure_stops_the_job_and_lists_are_in_diff_and_commit():
    steps = _workflow_steps()
    build = steps[_step_index(steps, "python scripts/main.py build-lists")]
    assert "continue-on-error" not in build and "\n        if:" not in build
    text = (REPO / ".github" / "workflows" / "update_parking_data.yml").read_text(encoding="utf-8")
    assert "LISTS_DIR: 'data/lists'" in text
    check = steps[_step_index(steps, "name: Check for changes")]
    commit = steps[_step_index(steps, "name: Create temporary branch and commit")]
    assert "git status --porcelain" in check and "LISTS_DIR" in check and "DATA_FILE" in check
    assert re.search(r"git add -A .*DATA_FILE.*LISTS_DIR", commit)


# ---- P3: aria-checked must be an explicit true/false ---------------------------------------------------
@pytest.mark.parametrize("value", ["<missing>", "mixed", "", "TRUE", "1"])
def test_p3_missing_or_invalid_aria_checked_is_a_failure_not_unchecked(value):
    p = FakePage()
    p.checked_attr = value
    with pytest.raises(ms.OpFailed, match="aria-checked"):
        ms.add_point(p, LIST_A, 25.0, 121.5)
    p2 = FakePage()
    p2.checked_attr = value
    p2.places["25.000000,121.500000"] = {LIST_A}
    with pytest.raises(ms.OpFailed, match="aria-checked"):
        ms.remove_point(p2, LIST_A, 25.0, 121.5)
    assert p2.places["25.000000,121.500000"] == {LIST_A}                     # nothing was clicked


# ---- P4: exactly one row / dialog / control before any mutation ----------------------------------------
def test_p4_two_rows_with_the_exact_name_fail_with_the_count_and_click_nothing():
    p = FakePage(lists=[LIST_A, LIST_A, LIST_B])
    with pytest.raises(ms.OpFailed, match=r"found 2 \(LIST_ITEM\)"):
        ms.add_point(p, LIST_A, 25.0, 121.5)
    assert p.places == {}


def test_p4_missing_row_fails_with_count_zero():
    p = FakePage(lists=[LIST_B])
    with pytest.raises(ms.OpFailed, match=r"found 0 \(LIST_ITEM\)"):
        ms.remove_point(p, LIST_A, 25.0, 121.5)


def test_p4_rows_outside_the_dialog_are_ignored():
    p = FakePage()
    p.stray_rows = [LIST_A]                       # a lookalike row elsewhere on the page
    assert ms.add_point(p, LIST_A, 25.0, 121.5) == "added"
    assert p.places["25.000000,121.500000"] == {LIST_A}


def test_p4_two_dialogs_holding_list_rows_fail():
    p = FakePage()
    p.dialog_count = 2
    with pytest.raises(ms.OpFailed, match=r"found 2 \(SAVE_DIALOG\)"):
        ms.add_point(p, LIST_A, 25.0, 121.5)
    assert p.places == {}


def test_p4_two_save_buttons_fail_before_any_click():
    p = FakePage()
    p.save_button_count = 2
    with pytest.raises(ms.OpFailed, match=r"found 2 \(SAVE_BUTTON\)"):
        ms.add_point(p, LIST_A, 25.0, 121.5)
    assert not p.dialog_open and p.places == {}


def test_p4_list_creation_controls_must_be_unique_too():
    p = FakePage(lists=[OLD_BIG_LIST])
    p.extra_counts = {S.NEW_LIST_BUTTON: 2}
    with pytest.raises(ms.OpFailed, match=r"found 2 \(NEW_LIST_BUTTON\)"):
        ms.ensure_list(p, LIST_A, 25.0, 121.5)
    assert p.list_names == [OLD_BIG_LIST]


# ---- P5: list creation obeys --max-ops -----------------------------------------------------------------
def test_p5_ensure_does_not_run_with_max_ops_zero(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(3)})
    ops = FakeOps()
    code, sp, logp = run(tmp_path, d, ops, max_ops=0)
    assert code == 2 and ops.calls == [] and not sp.exists()
    assert any(l.get("event") == "skipped" for l in log_lines(logp))


def test_p5_ensure_counts_as_one_op(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(3)})
    ops = FakeOps()
    code, sp, logp = run(tmp_path, d, ops, max_ops=1)
    assert code == 2 and [c[0] for c in ops.calls] == ["ensure"]
    assert read_state(sp)["lists"][LIST_A] == {"exists": True, "points": {}}
    assert log_lines(logp)[-1]["remaining"] == 3


# ---- P6: sign-in / consent pages seen mid-flow exit 3 --------------------------------------------------
def test_p6_consent_page_after_new_list_click_exits_3_not_a_selector_failure(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(2)})
    page = FakePage(lists=[OLD_BIG_LIST])
    page.new_list_goes_to = "https://consent.google.com/m?continue=x"
    code, sp, logp = run(tmp_path, d, ms.PageOps(page))
    assert code == 3 and log_lines(logp)[-1]["reason"] == "blocked"
    assert not sp.exists() or LIST_A not in read_state(sp)["lists"]


def test_p6_ops_themselves_check_the_page_before_reporting_a_missing_control():
    # without the PageOps wrapper: ensure_list after "New list", and the Saved tab in count_list
    page = FakePage(lists=[OLD_BIG_LIST])
    page.new_list_goes_to = "https://consent.google.com/m?continue=x"
    with pytest.raises(ms.Blocked):
        ms.ensure_list(page, LIST_A, 25.0, 121.5)
    page2 = FakePage()
    page2.extra_counts = {S.SAVED_NAV: 1}
    page2.click_to = {S.SAVED_NAV: "https://accounts.google.com/v3/signin/identifier"}
    with pytest.raises(ms.ReloginNeeded):
        ms.count_list(page2, LIST_A)


def test_p6_browser_error_on_a_sign_in_page_exits_3(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(2)})
    page = FakePage()
    page.click_error = (S.SAVE_BUTTON, TimeoutError("locator.click: timeout"),
                        "https://accounts.google.com/v3/signin/identifier")
    ops = ms.PageOps(page)
    code, sp, logp = run(tmp_path, d, ops)
    assert code == 3 and log_lines(logp)[-1]["reason"] == "relogin needed"


def test_p6_playwright_error_on_a_normal_page_is_still_an_op_failure(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(4)})
    ms.write_state_atomic(tmp_path / "state" / "sync_state.json", {"version": 1, "lists": {LIST_A: {"exists": True}}})
    page = FakePage()
    page.click_error = (S.SAVE_BUTTON, TimeoutError("locator.click: timeout"), page.url)
    code, _, _ = run(tmp_path, d, ms.PageOps(page))
    assert code == 4                                                         # 3 consecutive failures, not a relogin


def test_p6_count_path_consent_page_exits_3(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(1)})
    page = FakePage()
    page.extra_counts = {S.SAVED_NAV: 1}
    page.click_to = {S.SAVED_NAV: "https://consent.google.com/m?continue=x"}
    code = ms.sync_lists(lists_dir=d, state=str(tmp_path / "s.json"), count=LIST_A,
                         ops_factory=factory(ms.PageOps(page)))
    assert code == 3


def test_p6_count_path_with_a_missing_tab_on_a_normal_page_is_exit_1(tmp_path):
    d = make_lists(tmp_path, {LIST_A: pts(1)})
    code = ms.sync_lists(lists_dir=d, state=str(tmp_path / "s.json"), count=LIST_A,
                         ops_factory=factory(ms.PageOps(FakePage())))
    assert code == 1


# ---- P7: sync.log alone reconstructs a run -------------------------------------------------------------
def test_p7_every_op_logs_its_intent_before_dispatch_then_its_outcome(tmp_path):
    d = make_lists(tmp_path, {LIST_A: SHARED + [SOLO]})
    run(tmp_path, d, FakeOps(fail_at={2}))
    lines = log_lines(tmp_path / "state" / "sync.log")
    seen = []
    for l in lines:
        if l.get("event") == "intent":
            seen.append(l)
        elif l.get("event") == "op" and l["op"] in ("ensure", "add", "remove"):
            intent = seen.pop()                                              # the one just before this outcome
            assert (intent["list"], intent["place"], intent["op"]) == (l["list"], l["place"], l["op"])
    assert not seen
    intents = [l for l in lines if l.get("event") == "intent"]
    assert [i["op"] for i in intents] == ["ensure", "add", "add"]
    assert set(intents[1]["ids"]) == {shared_pid("#aaa"), shared_pid("#bbb"), shared_pid("#ccc")}
    assert intents[1]["place"] == "25.000000,121.500000"
    fail = [l for l in lines if l.get("result") == "fail"]
    assert len(fail) == 1 and "scripted failure" in fail[0]["reason"]


def test_p7_skipped_entities_and_state_write_failures_are_logged(tmp_path, monkeypatch):
    # ensure fails -> the list's ops are skipped, one line each
    d = make_lists(tmp_path, {LIST_A: pts(2)})

    class Ops(FakeOps):
        def ensure_list(self, task):
            raise ms.OpFailed("no new list button")

    run(tmp_path, d, Ops())
    skipped = [l for l in log_lines(tmp_path / "state" / "sync.log") if l.get("event") == "skipped"]
    assert len(skipped) == 2 and all("ensured" in l["reason"] for l in skipped)

    # max-ops stop -> a skipped line saying where it stopped
    t2 = tmp_path / "second"
    t2.mkdir()
    d2 = make_lists(t2, {LIST_A: pts(4)})
    run(t2, d2, FakeOps(), max_ops=2)
    assert any("max-ops" in l["reason"] for l in log_lines(t2 / "state" / "sync.log") if l.get("event") == "skipped")

    # state point without coordinates -> skipped, not silently dropped
    t3 = tmp_path / "third"
    t3.mkdir()
    d3 = make_lists(t3, {LIST_A: [SOLO]})
    st = {"version": 1, "lists": {LIST_A: {"exists": True, "points": {
        "TPE:1.000000,1.000000": {"road": "x"},
        pid(25.5): {"lat": 25.5, "lon": 121.5, "road": "solo", "payload_hash": payload_hash("solo", 25.5, 121.5)}}}}}
    ms.write_state_atomic(t3 / "state" / "sync_state.json", st)
    run(t3, d3, FakeOps(), state=t3 / "state" / "sync_state.json")
    sk = [l for l in log_lines(t3 / "state" / "sync.log") if l.get("event") == "skipped"]
    assert sk and sk[0]["point_id"] == "TPE:1.000000,1.000000" and "no coordinates" in sk[0]["reason"]

    # state write failure -> a dedicated line, outcome says the state was not saved, exit 1
    t4 = tmp_path / "fourth"
    t4.mkdir()
    d4 = make_lists(t4, {LIST_A: pts(2)})
    monkeypatch.setattr(ms, "write_state_atomic", lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")))
    code, _, logp = run(t4, d4, FakeOps())
    lines = log_lines(logp)
    assert code == 1
    assert any(l.get("event") == "state_write_failed" and "disk full" in l["reason"] for l in lines)
    assert any(l.get("result") == "ok_state_not_saved" for l in lines)
    assert lines[-1]["exit"] == 1


needs_bash = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available")


def _run_wrapper(tmp_path, git_rc=0, sync_rc=0, alert=True, args=("--dry-run",)):
    stubs = tmp_path / "bin"
    stubs.mkdir()
    marker = tmp_path / "xvfb_args"
    (stubs / "git").write_text(
        '#!/bin/sh\ncase "$*" in *pull*) echo "${FAKE_GIT_MSG:-Already up to date.}"; exit ${FAKE_GIT_RC:-0};; esac\n'
        'echo abc1234\n', encoding="utf-8")
    (stubs / "xvfb-run").write_text(f'#!/bin/sh\necho "$@" > {marker}\nexit ${{FAKE_RC:-0}}\n', encoding="utf-8")
    (stubs / "notify").write_text(f'#!/bin/sh\necho "$*" >> {tmp_path}/alerts\n', encoding="utf-8")
    for f in stubs.iterdir():
        f.chmod(0o755)
    env = {"PATH": f"{stubs}:/usr/bin:/bin", "HOME": str(tmp_path), "THP_STATE_DIR": str(tmp_path / "st"),
           "FAKE_GIT_RC": str(git_rc), "FAKE_RC": str(sync_rc), "FAKE_GIT_MSG": "fatal: Not possible to fast-forward"
           if git_rc else "Already up to date."}
    if alert:
        env["THP_ALERT_CMD"] = str(stubs / "notify")
    r = subprocess.run(["bash", str(REPO / "deploy" / "ubuntu" / "thp-maps-sync.sh"), *args],
                       env=env, capture_output=True, text=True)
    return r, [json.loads(x) for x in (tmp_path / "st" / "sync.log").read_text(encoding="utf-8").splitlines()], marker


@needs_bash
def test_p7_wrapper_logs_start_pull_and_exit_with_one_run_id(tmp_path):
    r, lines, marker = _run_wrapper(tmp_path, sync_rc=3)
    assert r.returncode == 3
    assert [(l["stage"], l["rc"]) for l in lines] == [("start", 0), ("pull", 0), ("exit", 3), ("alert", 0)]
    assert len({l["run_id"] for l in lines}) == 1 and all(l["event"] == "wrapper" for l in lines)
    assert "Already up to date." in lines[1]["detail"]
    run_id = lines[0]["run_id"]
    assert f"--run-id {run_id} --dry-run" in marker.read_text(encoding="utf-8")     # sync lines share the id
    assert "thp-maps-sync exit 3" in (tmp_path / "alerts").read_text(encoding="utf-8")


@needs_bash
def test_wrapper_passes_its_state_dir_to_sync_lists_so_one_log_holds_the_run(tmp_path):
    r, lines, marker = _run_wrapper(tmp_path)
    assert r.returncode == 0
    assert f"--state {tmp_path / 'st' / 'sync_state.json'} --run-id" in marker.read_text(encoding="utf-8")


@needs_bash
@pytest.mark.parametrize("bad", [["--state", "/tmp/x.json"], ["--state=/tmp/x.json"]])
def test_wrapper_refuses_a_state_argument_and_does_not_sync(tmp_path, bad):
    r, lines, marker = _run_wrapper(tmp_path, args=bad)
    assert r.returncode == 1 and not marker.exists()
    assert [(l["stage"], l["rc"]) for l in lines] == [("start", 0), ("args", 1), ("exit", 1)]


@pytest.mark.parametrize("abbrev", ["--sta", "--stat", "--sta=/tmp/x.json"])
def test_cli_refuses_abbreviated_state_so_the_wrapper_guard_cannot_be_bypassed(abbrev):
    r = subprocess.run([sys.executable, str(REPO / "scripts" / "main.py"), "sync-lists", "--dry-run",
                        *([abbrev] if "=" in abbrev else [abbrev, "/tmp/x.json"])], capture_output=True, text=True)
    assert r.returncode != 0 and "unrecognized arguments" in r.stderr


@needs_bash
def test_wrapper_resolves_a_relative_state_dir_once_so_one_log_holds_the_run(tmp_path):
    stubs = tmp_path / "bin"
    stubs.mkdir()
    marker = tmp_path / "xvfb_args"
    (stubs / "git").write_text('#!/bin/sh\necho "Already up to date."\n', encoding="utf-8")
    (stubs / "xvfb-run").write_text(f'#!/bin/sh\necho "$@" > {marker}\n', encoding="utf-8")
    for f in stubs.iterdir():
        f.chmod(0o755)
    env = {"PATH": f"{stubs}:/usr/bin:/bin", "HOME": str(tmp_path), "THP_STATE_DIR": "rel-st"}
    r = subprocess.run(["bash", str(REPO / "deploy" / "ubuntu" / "thp-maps-sync.sh"), "--dry-run"],
                       env=env, cwd=tmp_path, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    st = (tmp_path / "rel-st").resolve()
    stages = [json.loads(x)["stage"] for x in (st / "sync.log").read_text(encoding="utf-8").splitlines()]
    assert stages == ["start", "pull", "exit"]
    assert f"--state {st / 'sync_state.json'} --run-id" in marker.read_text(encoding="utf-8")


@needs_bash
def test_p7_wrapper_logs_a_failed_pull_and_does_not_sync(tmp_path):
    r, lines, marker = _run_wrapper(tmp_path, git_rc=1)
    assert r.returncode == 1
    assert [(l["stage"], l["rc"]) for l in lines] == [("start", 0), ("pull", 1), ("exit", 1)]
    assert "Not possible to fast-forward" in lines[1]["detail"]
    assert not marker.exists()                                               # xvfb-run / sync never started


@needs_bash
def test_p7_wrapper_without_alert_command_says_so_in_the_log(tmp_path):
    r, lines, _ = _run_wrapper(tmp_path, sync_rc=4, alert=False)
    assert r.returncode == 4 and lines[-1]["stage"] == "alert" and "no alert sent" in lines[-1]["detail"]


# =====================================================================================================
# Live DOM facts (CTO probe, English UI): one test per selector change. Each fails on the old constant.
# =====================================================================================================
def _place_with(on_list):
    p = FakePage()
    p.goto(S.place_url(25.0, 121.5))
    if on_list:
        p.places["25.000000,121.500000"] = {LIST_A}
    return p


@pytest.mark.parametrize("on_list", [False, True])
def test_live_save_button_is_data_value_save_or_saved_and_matches_exactly_once(on_list):
    p = _place_with(on_list)
    buttons = [e for e in p.elements() if matches(e, S.SAVE_BUTTON)]
    assert len(buttons) == 1
    assert buttons[0].attrs["data-value"] == ("Saved" if on_list else "Save")
    assert ms.add_point(_place_with(on_list), LIST_B, 25.0, 121.5) == "added"      # the click path works too


def test_live_save_menu_is_the_role_menu_and_ignores_unrelated_dialogs():
    p = FakePage()
    p.dialog_lookalike_rows = True            # one of the page's role=dialog elements also holds radio rows
    assert ms.add_point(p, LIST_A, 25.0, 121.5) == "added"
    assert p.places["25.000000,121.500000"] == {LIST_A}


@pytest.mark.parametrize("role", ["menuitemradio", "menuitemcheckbox"])
def test_live_list_rows_are_menuitemradio_and_menuitemcheckbox_is_still_accepted(role):
    p = FakePage()
    p.row_role = role
    assert ms.add_point(p, LIST_A, 25.0, 121.5) == "added"
    assert ms.remove_point(p, LIST_A, 25.0, 121.5) == "removed"


@pytest.mark.parametrize("index", [0, 1, 2])
def test_live_name_is_matched_on_any_line_of_the_row_text(index):
    # row text shapes seen live: ["","",name,"Shared · N places"], [name,"Private · N places"],
    # [glyph,name,"Private · N places"]; the shape follows the row index in the fake
    names = ["x"] * index + [LIST_A]
    p = FakePage(lists=names)
    lines = p.row_lines(index, LIST_A)
    assert LIST_A in lines and (lines[0] == LIST_A) == (index == 1)
    assert ms.add_point(p, LIST_A, 25.0, 121.5) == "added"
    assert p.places["25.000000,121.500000"] == {LIST_A}


def test_live_name_match_is_never_a_substring_of_a_line():
    p = FakePage(lists=[LIST_A + " 備份", "舊 " + LIST_A, LIST_B])
    with pytest.raises(ms.OpFailed, match="found 0"):
        ms.add_point(p, LIST_A, 25.0, 121.5)
    assert p.places == {}


def test_live_new_list_form_input_is_the_one_text_input_inside_the_create_dialog():
    p = FakePage(lists=[OLD_BIG_LIST])
    # the page also has a combobox text input and textareas outside any dialog, and 2 unrelated dialogs
    assert ms.ensure_list(p, LIST_A, 25.0, 121.5) == "created"
    assert p.list_names[-1] == LIST_A


def test_live_new_list_form_with_two_text_inputs_is_refused():
    p = FakePage(lists=[OLD_BIG_LIST])
    p.extra_form_inputs = 1
    with pytest.raises(ms.OpFailed, match=r"found 2 \(LIST_NAME_INPUT\)"):
        ms.ensure_list(p, LIST_A, 25.0, 121.5)
    assert p.list_names == [OLD_BIG_LIST]


@pytest.mark.parametrize("variant", ["both", "data-value", "aria-label", "icon-only"])
def test_live_create_button_is_found_by_data_value_or_aria_label_not_by_its_text(variant):
    p = FakePage(lists=[OLD_BIG_LIST])
    p.create_variant = variant
    assert ms.ensure_list(p, LIST_A, 25.0, 121.5) == "created"


def test_live_create_dialog_is_the_one_role_dialog_that_holds_the_create_button():
    p = FakePage(lists=[OLD_BIG_LIST])
    p.goto(S.place_url(25.0, 121.5))
    p.creating = True
    dialogs = [e for e in p.elements() if matches(e, S.CREATE_DIALOG)]
    holding = p.locator(S.CREATE_DIALOG, has=p.locator(S.CREATE_LIST_CONFIRM)).count()
    assert len(dialogs) == 3 and holding == 1
