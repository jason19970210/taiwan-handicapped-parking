"""Per-city row-count guard and fail-loud merger. No network."""

import json
import logging

import pandas as pd
import pytest

import scripts.main as main_mod
from scripts.data_collection.merger import DataMerger
from scripts.utils.count_guard import check_city_counts, city_counts, read_committed_counts

COLS = ["city", "area", "road", "dd_lat", "dd_long", "dms_lat", "dms_long"]


def frame(tp, ntpc):
    rows = []
    for i in range(tp):
        rows.append(["Taipei City", "中山區", f"r{i}", 25.0 + i * 1e-4, 121.5, "", ""])
    for i in range(ntpc):
        rows.append(["New Taipei City", "板橋區", f"r{i}", 25.0 + i * 1e-4, 121.4, "", ""])
    return pd.DataFrame(rows, columns=COLS)


# ---- pure guard ------------------------------------------------------------

def test_city_counts():
    assert city_counts(frame(3, 5)) == {"Taipei City": 3, "New Taipei City": 5}


def test_small_change_passes():
    old = {"Taipei City": 1695, "New Taipei City": 914}
    new = {"Taipei City": 1690, "New Taipei City": 900}  # -0.3%, -1.5%
    assert check_city_counts(old, new) == []


def test_drop_over_threshold_is_flagged_and_mentions_numbers():
    old = {"Taipei City": 1695, "New Taipei City": 31134}
    new = {"Taipei City": 1695, "New Taipei City": 11829}
    problems = check_city_counts(old, new)
    assert len(problems) == 1
    assert "New Taipei City" in problems[0] and "31134" in problems[0] and "11829" in problems[0]


def test_drop_exactly_at_threshold_passes_and_configurable():
    old = {"A": 1000}
    assert check_city_counts(old, {"A": 950}, max_drop_pct=5) == []
    assert check_city_counts(old, {"A": 949}, max_drop_pct=5)
    assert check_city_counts(old, {"A": 900}, max_drop_pct=20) == []


def test_missing_city_counts_as_total_drop():
    assert check_city_counts({"A": 10, "B": 10}, {"A": 10})


def test_new_city_and_empty_baseline_pass():
    assert check_city_counts({}, {"A": 5}) == []
    assert check_city_counts({"A": 0}, {"A": 5}) == []
    assert check_city_counts({"A": 10}, {"A": 10, "B": 3}) == []


def test_runaway_growth_is_flagged():
    """The unfiltered-API bug showed up as +3300%, which nobody noticed."""
    assert check_city_counts({"A": 914}, {"A": 31000})
    assert check_city_counts({"A": 914}, {"A": 31000}, max_growth_pct=None) == []


def test_read_committed_counts(tmp_path):
    p = tmp_path / "x.csv"
    frame(2, 3).to_csv(p, index=False, encoding="utf-8-sig")
    assert read_committed_counts(str(p)) == {"Taipei City": 2, "New Taipei City": 3}
    assert read_committed_counts(str(tmp_path / "missing.csv")) == {}


# ---- collect_data wiring -----------------------------------------------------

@pytest.fixture
def env(tmp_path, monkeypatch):
    out = tmp_path / "parking_locations.csv"
    cfg = tmp_path / "data_sources.json"
    cfg.write_text(
        json.dumps({"sources": [], "output": {"file": str(out), "columns": COLS}}),
        encoding="utf-8",
    )
    committed = frame(100, 100)
    committed.to_csv(out, index=False, encoding="utf-8-sig")

    state = {"df": None, "calls": 0, "log": []}

    class FakeMerger:
        def __init__(self, config):
            pass

        def collect_and_merge(self):
            state["calls"] += 1
            return state["df"]

    class ListHandler(logging.Handler):
        def emit(self, record):
            state["log"].append(record.getMessage())

    handler = ListHandler(level=logging.DEBUG)
    main_mod.logger.addHandler(handler)
    monkeypatch.setattr(main_mod, "DataMerger", FakeMerger)
    yield out, str(cfg), state
    main_mod.logger.removeHandler(handler)


def test_collect_aborts_without_writing_on_big_drop(env):
    out, cfg, state = env
    before = out.read_bytes()
    state["df"] = frame(100, 40)
    assert main_mod.collect_data(config_path=cfg) == main_mod.EXIT_GUARD_ABORT == 2
    assert state["calls"] == 1  # collection ran...
    assert out.read_bytes() == before  # ...but nothing was written
    log = "\n".join(state["log"])
    assert "'Taipei City': 100, 'New Taipei City': 100" in log   # committed counts
    assert "'New Taipei City': 40" in log                        # collected counts
    assert "COUNT GUARD ABORT" in log and "New Taipei City: 100 -> 40 rows (-60.0%" in log


def test_collect_writes_on_small_change(env):
    out, cfg, state = env
    state["df"] = frame(99, 103)
    assert main_mod.collect_data(config_path=cfg) == 0
    assert state["calls"] == 1
    assert city_counts(pd.read_csv(out)) == {"Taipei City": 99, "New Taipei City": 103}


def test_accept_flag_overrides_guard(env):
    out, cfg, state = env
    state["df"] = frame(100, 40)
    assert main_mod.collect_data(config_path=cfg, accept_count_change=True) == 0
    assert city_counts(pd.read_csv(out))["New Taipei City"] == 40


def test_threshold_is_configurable(env):
    out, cfg, state = env
    state["df"] = frame(100, 70)
    assert main_mod.collect_data(config_path=cfg, max_drop_pct=40) == 0


def test_cli_exit_code_is_nonzero_on_abort(env, monkeypatch):
    out, cfg, state = env
    state["df"] = frame(100, 10)
    before = out.read_bytes()
    monkeypatch.setattr("sys.argv", ["main.py", "collect", "--config", cfg])
    with pytest.raises(SystemExit) as e:
        main_mod.main()
    assert e.value.code == 2
    assert state["calls"] == 1
    assert out.read_bytes() == before
    assert "'New Taipei City': 10" in "\n".join(state["log"])


def test_collect_fails_when_merger_raises(env, monkeypatch):
    out, cfg, state = env
    before = out.read_bytes()

    class Boom:
        def __init__(self, config):
            pass

        def collect_and_merge(self):
            raise RuntimeError("source failed")

    monkeypatch.setattr(main_mod, "DataMerger", Boom)
    assert main_mod.collect_data(config_path=cfg) == 1  # plain failure, not the guard code
    assert out.read_bytes() == before
    assert "source failed" in "\n".join(state["log"])


# ---- merger fails loudly ----------------------------------------------------

class StubHandler:
    def __init__(self, df=None, exc=None):
        self.df, self.exc = df, exc

    def process(self):
        if self.exc:
            raise self.exc
        return self.df


def merger_with(handlers, monkeypatch):
    sources = [{"id": k, "name": k, "enabled": True, "handler": k} for k in handlers]
    m = DataMerger({"sources": sources, "output": {"deduplication_keys": []}})
    monkeypatch.setattr(m, "_load_handler", lambda src: handlers[src["id"]])
    return m


def test_merger_raises_if_any_source_fails(monkeypatch):
    m = merger_with(
        {"a": StubHandler(frame(3, 0)), "b": StubHandler(exc=RuntimeError("down"))}, monkeypatch
    )
    with pytest.raises(RuntimeError):
        m.collect_and_merge()


def test_merger_raises_if_any_source_is_empty(monkeypatch):
    m = merger_with({"a": StubHandler(frame(3, 0)), "b": StubHandler(frame(0, 0))}, monkeypatch)
    with pytest.raises(RuntimeError):
        m.collect_and_merge()


def test_merger_ok_when_all_sources_deliver(monkeypatch):
    m = merger_with({"a": StubHandler(frame(3, 0)), "b": StubHandler(frame(0, 2))}, monkeypatch)
    assert len(m.collect_and_merge()) == 5
