"""
Pagination, retry and filter behaviour of the New Taipei handler.
All HTTP is faked by monkeypatching requests.get; nothing touches the network.
"""

import pytest
import requests

import scripts.data_collection.new_taipei_handler as nth
from scripts.data_collection.new_taipei_handler import NewTaipeiHandler, PaginationError

HEADER = (
    '"id","cellid","name","day","hour","pay","paycash","memo","roadid",'
    '"roadname","cellstatus","isnowcash","parkingstatus","latitude","longitude",'
    '"countycode","areacode"'
)
HANDICAPPED = "汽車身心障礙專用"
NORMAL = "汽車停車位"


def make_csv(n, start=0, name=HANDICAPPED, header=HEADER):
    rows = [header]
    for i in range(start, start + n):
        rows.append(
            f'"{i}","{i}","{name}","","","","","","K{i}","路{i}","Y","true","1",'
            f'"{25.0 + i * 1e-5:.6f}","{121.4 + i * 1e-5:.6f}","65000","65000010"'
        )
    return "\n".join(rows) + "\n"


class FakeResponse:
    def __init__(self, text, status=200):
        self.text = text
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeServer:
    """A fake paginated API serving `total` rows (all matching the filter).

    faults maps (page, nth_request_of_that_page) -> outcome, where outcome is
    an Exception (raised), a str (returned as the 200 body) or a FakeResponse.
    Pages past the end are empty, like the real API. This mirrors the live
    failures seen on 2026-11-04: empty 200 bodies and short pages mid-run.
    """

    def __init__(self, total, page_size=10, faults=None):
        self.total, self.size = total, page_size
        self.faults = dict(faults or {})
        self.seen = {}
        self.calls = []

    def page_body(self, page, size=None):
        size = size or self.size
        start = page * size
        n = max(0, min(size, self.total - start))
        return make_csv(n, start) if n else ""

    def __call__(self, url, timeout=None, **kw):
        page = int(url.split("page=")[1].split("&")[0])
        size = int(url.split("size=")[1])
        if size != self.size:
            # a request with another page size (the end cross-check): never faulted
            self.calls.append(("alt", page, size))
            return FakeResponse(self.page_body(page, size))
        self.calls.append(page)
        nth = self.seen[page] = self.seen.get(page, 0) + 1
        outcome = self.faults.get((page, nth))
        if outcome is None:
            return FakeResponse(self.page_body(page))
        if isinstance(outcome, Exception):
            raise outcome
        if isinstance(outcome, FakeResponse):
            return outcome
        return FakeResponse(outcome)


def make_handler(tmp_path, **cfg):
    config = {
        "url": "https://example.test/api/csv",
        "format": "csv_paginated",
        "coordinate_system": "WGS84",
        "page_size": 10,
        "filter_field": "NAME",  # deliberately upper case: the live API is lower case
        "filter_pattern": HANDICAPPED,
        "max_retries": 3,
        "fields_mapping": {
            "city": "fixed:New Taipei City",
            "area": "AreaCode",
            "road": "ROADNAME",
            "lat": "latitude",
            "lon": "longitude",
        },
    }
    config.update(cfg)
    h = NewTaipeiHandler(config, source_id="new_taipei_city")
    h.cache_dir = tmp_path / "cache"
    h.debug_dir = tmp_path / "debug"
    return h


def run(handler, server, monkeypatch):
    monkeypatch.setattr(nth.requests, "get", server)
    return handler.fetch_data()


def test_full_run_collects_all_rows(tmp_path, monkeypatch):
    server = FakeServer(total=24)
    df = run(make_handler(tmp_path), server, monkeypatch)
    assert len(df) == 24
    assert sorted(c for c in set(server.calls) if isinstance(c, int)) == [0, 1, 2, 3]  # page 3 only to confirm the end
    assert any(isinstance(c, tuple) for c in server.calls)  # end cross-check at another page size


def test_exact_multiple_of_page_size_is_accepted_when_confirmed(tmp_path, monkeypatch):
    """Healthy total that is an exact multiple of the page size: the empty page after a full
    page is the end once it is empty again on re-read AND the size-9 cross-check agrees."""
    df = run(make_handler(tmp_path), FakeServer(total=20), monkeypatch)
    assert len(df) == 20
    assert len(list((tmp_path / "cache").glob("*.pkl"))) == 1


def test_exact_multiple_with_live_page_size_1000(tmp_path, monkeypatch):
    server = FakeServer(total=2000, page_size=1000)
    df = run(make_handler(tmp_path, page_size=1000), server, monkeypatch)
    assert len(df) == 2000


def test_empty_page_whose_reread_returns_rows_is_not_the_end(tmp_path, monkeypatch):
    """Every first read of page 2 is empty but every re-read has rows: never confirmed."""
    faults = {(2, n): ("" if n % 2 else make_csv(5, 20)) for n in range(1, 500)}
    with pytest.raises(PaginationError):
        run(make_handler(tmp_path, max_retries=3), FakeServer(25, faults=faults), monkeypatch)
    assert not list((tmp_path / "cache").glob("*.pkl"))


def test_empty_page_after_full_pages_fails_when_cross_check_disagrees(tmp_path, monkeypatch):
    """Page 2 is persistently empty but the size-9 view shows rows at offset 18..: not the end."""
    server = FakeServer(total=25, faults={(2, n): "" for n in range(1, 500)})
    with pytest.raises(PaginationError):
        run(make_handler(tmp_path, max_retries=3), server, monkeypatch)
    assert not list((tmp_path / "cache").glob("*.pkl"))


def test_transient_connection_error_mid_run_is_retried(tmp_path, monkeypatch):
    server = FakeServer(24, faults={(2, 1): requests.ConnectionError("Connection reset by peer")})
    df = run(make_handler(tmp_path), server, monkeypatch)
    assert len(df) == 24
    assert server.seen[2] >= 2


def test_http_500_and_garbage_body_are_retried(tmp_path, monkeypatch):
    server = FakeServer(
        24,
        faults={
            (1, 1): FakeResponse("boom", status=500),
            (1, 2): "<html><body>Request rejected</body></html>",
        },
    )
    assert len(run(make_handler(tmp_path), server, monkeypatch)) == 24


def test_empty_page_mid_run_is_not_the_end(tmp_path, monkeypatch):
    """Live 2026-11-04: the API answered 200 with an empty body mid-run."""
    server = FakeServer(24, faults={(1, 1): "", (1, 2): ""})
    assert len(run(make_handler(tmp_path), server, monkeypatch)) == 24


def test_sustained_outage_shorter_than_retry_budget_recovers(tmp_path, monkeypatch):
    """Live 2026-11-04: pages 18-31 came back empty for 14+ consecutive requests."""
    server = FakeServer(24, faults={(2, n): "" for n in range(1, 5)})
    assert len(run(make_handler(tmp_path, max_retries=6), server, monkeypatch)) == 24


def test_sustained_outage_longer_than_retry_budget_raises(tmp_path, monkeypatch):
    server = FakeServer(24, faults={(2, n): "" for n in range(1, 200)})
    with pytest.raises(PaginationError):
        run(make_handler(tmp_path, max_retries=3), server, monkeypatch)
    assert not list((tmp_path / "cache").glob("*.pkl"))  # nothing cached from a failed run


def test_short_page_mid_run_is_a_glitch(tmp_path, monkeypatch):
    """Live 2026-11-04: pages came back with 881 and 823 rows instead of 1000."""
    server = FakeServer(35, faults={(1, 1): make_csv(7, 10)})
    assert len(run(make_handler(tmp_path), server, monkeypatch)) == 35


def test_truncated_last_page_is_not_accepted_as_complete(tmp_path, monkeypatch):
    """Last page returns 3 rows once, then the real 5: the first read must not stick."""
    server = FakeServer(35, faults={(3, 1): make_csv(3, 30)})
    assert len(run(make_handler(tmp_path), server, monkeypatch)) == 35


def test_short_page_that_never_recovers_raises(tmp_path, monkeypatch):
    faults = {(1, n): make_csv(7, 10) for n in range(1, 200)}
    with pytest.raises(PaginationError):
        run(make_handler(tmp_path, max_retries=2), FakeServer(35, faults=faults), monkeypatch)


def test_empty_first_page_raises(tmp_path, monkeypatch):
    with pytest.raises(PaginationError):
        run(make_handler(tmp_path), FakeServer(0), monkeypatch)


def test_missing_filter_column_raises_instead_of_keeping_everything(tmp_path, monkeypatch):
    h = make_handler(tmp_path, filter_field="no_such_column")
    with pytest.raises(PaginationError):
        run(h, FakeServer(24), monkeypatch)


def test_filter_matches_case_insensitive_column_names_and_filters(tmp_path, monkeypatch):
    """Live API uses lower case columns; config says NAME. Only matching rows survive."""
    server = FakeServer(24)
    original = server.page_body
    # every second row of every page is a normal (non-handicapped) space
    def mixed(page, size=None):
        body = original(page, size)
        if not body:
            return body
        lines = body.rstrip("\n").split("\n")
        out = [lines[0]] + [
            ln if i % 2 else ln.replace(HANDICAPPED, NORMAL) for i, ln in enumerate(lines[1:])
        ]
        return "\n".join(out) + "\n"
    server.page_body = mixed
    df = run(make_handler(tmp_path), server, monkeypatch)
    assert len(df) == 12
    assert set(df["name"]) == {HANDICAPPED}


def test_no_matching_rows_at_all_raises(tmp_path, monkeypatch):
    server = FakeServer(24)
    server.page_body = lambda page, size=None: make_csv(
        max(0, min(10, 24 - page * 10)), page * 10, name=NORMAL
    ) if page * 10 < 24 else ""
    with pytest.raises(PaginationError):
        run(make_handler(tmp_path), server, monkeypatch)


def test_process_populates_area_and_road_from_lowercase_columns(tmp_path, monkeypatch):
    h = make_handler(tmp_path)
    monkeypatch.setattr(nth.requests, "get", FakeServer(3))
    out = h.process()
    assert list(out["area"].unique()) == ["板橋區"]
    assert out["road"].tolist() == ["路0", "路1", "路2"]
    assert (out["city"] == "New Taipei City").all()


def test_cache_written_only_after_complete_run(tmp_path, monkeypatch):
    run(make_handler(tmp_path), FakeServer(11), monkeypatch)
    assert len(list((tmp_path / "cache").glob("*.pkl"))) == 1


# ---- review loop 1: identity checks, cross-check, deadline and shared budget -----

def test_consistently_truncated_last_page_is_rejected_by_cross_check(tmp_path, monkeypatch):
    """Page 3 always returns 3 of its 5 rows and page 4 is empty: length and emptiness repeat,
    but the data at another page size shows the real end."""
    faults = {(3, n): make_csv(3, 30) for n in range(1, 500)}
    with pytest.raises(PaginationError):
        run(make_handler(tmp_path, max_retries=3), FakeServer(35, faults=faults), monkeypatch)
    assert not list((tmp_path / "cache").glob("*.pkl"))


def test_same_length_but_different_rows_on_reread_is_not_accepted(tmp_path, monkeypatch):
    """First read of the last page returns 5 WRONG rows, every re-read the real 5: same length,
    so a length-only comparison would keep the wrong rows."""
    faults = {(3, 1): make_csv(5, 500)}
    df = run(make_handler(tmp_path), FakeServer(35, faults=faults), monkeypatch)
    assert len(df) == 35 and set(df["id"]) == {str(i) for i in range(35)}


def test_different_rows_on_every_reread_raises(tmp_path, monkeypatch):
    faults = {(3, n): make_csv(5, 30 if n % 2 else 500 + n) for n in range(1, 500)}
    with pytest.raises(PaginationError):
        run(make_handler(tmp_path, max_retries=3), FakeServer(35, faults=faults), monkeypatch)


def test_status_flip_between_rereads_is_not_a_content_change(tmp_path, monkeypatch):
    """parkingstatus is a live field; it must not count as identity."""
    flipped = make_csv(5, 30).replace('"1","25.0', '"2","25.0')
    server = FakeServer(35, faults={(3, 2): flipped})
    assert len(run(make_handler(tmp_path), server, monkeypatch)) == 35


def test_full_run_spends_no_retries_and_stays_far_from_deadline(tmp_path, monkeypatch):
    h = make_handler(tmp_path)
    run(h, FakeServer(35), monkeypatch)
    assert h._retries_left == h.retry_budget


def test_shared_retry_budget_spans_pages(tmp_path, monkeypatch):
    """Each page fails once (within its own per-page limit) but the crawl-wide budget is 3."""
    faults = {(p, 1): requests.ConnectionError("reset") for p in range(5)}
    h = make_handler(tmp_path, max_retries=6, retry_budget=3)
    with pytest.raises(PaginationError, match="retry budget"):
        run(h, FakeServer(45, faults=faults), monkeypatch)


def test_crawl_deadline_stops_a_slow_outage(tmp_path, monkeypatch):
    """Backoff sleeps advance a fake clock; the 100 s crawl deadline must cut the retries off
    long before max_retries * pages would."""
    now = [0.0]
    monkeypatch.setattr("time.sleep", lambda secs: now.__setitem__(0, now[0] + secs))
    h = make_handler(tmp_path, max_retries=50, retry_budget=10_000, crawl_deadline_seconds=100,
                     retry_backoff_seconds=10, retry_backoff_max_seconds=30)
    h._now = lambda: now[0]
    faults = {(1, n): "" for n in range(1, 10_000)}
    with pytest.raises(PaginationError, match="deadline"):
        run(h, FakeServer(45, faults=faults), monkeypatch)
    assert now[0] <= 100 + 1e-6  # sleeps are clipped to the remaining time


def test_deadline_is_enforced_after_a_slow_request_that_then_succeeds(tmp_path, monkeypatch):
    """The LAST request (the end cross-check) 'takes' longer than the whole deadline and then
    returns a good page: the crawl must still abort, and nothing is cached."""
    now = [0.0]
    server = FakeServer(24)

    def slow(url, timeout=None, **kw):
        if "size=9" in url:
            now[0] += 150  # the request itself outlives the 100 s deadline
        return server(url, timeout=timeout, **kw)

    h = make_handler(tmp_path, crawl_deadline_seconds=100)
    h._now = lambda: now[0]
    monkeypatch.setattr(nth.requests, "get", slow)
    with pytest.raises(PaginationError, match="deadline"):
        h.fetch_data()
    assert not list((tmp_path / "cache").glob("*.pkl"))


def test_connect_timeout_is_clipped_to_the_remaining_deadline(tmp_path, monkeypatch):
    seen = []
    server = FakeServer(5)

    def spy(url, timeout=None, **kw):
        seen.append(timeout)
        return server(url, timeout=timeout, **kw)

    h = make_handler(tmp_path, crawl_deadline_seconds=5)
    h._now = lambda: 0.0  # frozen clock: exactly 5 s remain on every request
    monkeypatch.setattr(nth.requests, "get", spy)
    h.fetch_data()
    assert all(c <= 5.0 and r >= 1 for c, r in seen)
    assert seen[0][0] == 5.0  # connect timeout would be 10 without the clip


# ---- review loop 3: end check at an exact multiple of page size AND cross-check size ----
# page_size 1000 -> cross-check size 900 -> 9000 rows is a multiple of both, so the
# cross-check page that holds offset 9000 is expected to be empty. An empty reply alone
# must never satisfy it: that is what an all-empty outage looks like.

class TruncatedAtNineThousand(FakeServer):
    """Serves 9 full pages of 1000, then answers empty for every offset at every size."""

    def __init__(self):
        super().__init__(total=9000, page_size=1000)

    def __call__(self, url, timeout=None, **kw):
        size = int(url.split("size=")[1])
        if size != self.size:
            self.calls.append(("alt", int(url.split("page=")[1].split("&")[0]), size))
            return FakeResponse("")
        return super().__call__(url, timeout=timeout, **kw)


def test_all_empty_outage_at_nine_thousand_rows_raises(tmp_path, monkeypatch):
    h = make_handler(tmp_path, page_size=1000, max_retries=3)
    with pytest.raises(PaginationError, match="cross-check"):
        run(h, TruncatedAtNineThousand(), monkeypatch)
    assert not list((tmp_path / "cache").glob("*.pkl"))
    assert not list((tmp_path / "debug").glob("*"))  # no CSV written either


def test_healthy_nine_thousand_rows_passes_with_positive_tail_evidence(tmp_path, monkeypatch):
    server = FakeServer(total=9000, page_size=1000)
    df = run(make_handler(tmp_path, page_size=1000), server, monkeypatch)
    assert len(df) == 9000
    assert ("alt", 10, 900) in server.calls  # the page past the end is empty ...
    assert ("alt", 9, 900) in server.calls   # ... and the last FULL size-900 page confirms the tail


def test_nine_thousand_rows_whose_last_full_alt_page_has_other_rows_is_rejected(tmp_path, monkeypatch):
    server = FakeServer(total=9000, page_size=1000)
    original = server.page_body
    server.page_body = lambda page, size=None: (
        make_csv(900, 50_000) if (size, page) == (900, 9) else original(page, size)
    )
    with pytest.raises(PaginationError, match="cross-check"):
        run(make_handler(tmp_path, page_size=1000, max_retries=3), server, monkeypatch)
    assert not list((tmp_path / "cache").glob("*.pkl"))
