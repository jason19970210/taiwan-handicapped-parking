"""District assignment, list splitting and the sync plan. No network, no Google."""

import json
import random

import pandas as pd
import pytest

from scripts.lists.builder import build_lists, split_into_lists
from scripts.lists.districts import assign_districts, load_boundaries
from scripts.lists.sync_plan import assign_point_ids, make_plan, point_id

REAL_BOUNDARIES = "data/boundaries/tw_districts_tpe_ntpc.geojson"
COLS = ["city", "area", "road", "dd_lat", "dd_long", "dms_lat", "dms_long"]


def rows_df(rows):
    return pd.DataFrame(rows, columns=COLS)


def test_boundary_file_has_41_polygons_source_and_is_small():
    import os

    b = load_boundaries(REAL_BOUNDARIES)
    assert len(b.polygons) == 41
    assert b.metadata["source_url"].startswith("https://")
    assert b.metadata["downloaded"]
    assert os.path.getsize(REAL_BOUNDARIES) < 1_000_000


def test_point_in_polygon_known_points_one_per_city():
    b = load_boundaries(REAL_BOUNDARIES)
    df = rows_df(
        [
            ["Taipei City", "", "x", 25.0339, 121.5645, "", ""],  # Taipei 101
            ["New Taipei City", "", "x", 25.0143, 121.4638, "", ""],  # Banqiao station
        ]
    )
    out = assign_districts(df, b)
    assert out.loc[0, "district"] == "信義區" and out.loc[0, "district_city"] == "Taipei City"
    assert out.loc[1, "district"] == "板橋區" and out.loc[1, "district_city"] == "New Taipei City"


def test_point_outside_every_polygon_is_reported_not_dropped():
    b = load_boundaries(REAL_BOUNDARIES)
    df = rows_df(
        [
            ["New Taipei City", "三重區", "sea", 25.30, 121.90, "", ""],  # open sea
            ["New Taipei City", "", "sea2", 25.31, 121.91, "", ""],
        ]
    )
    out = assign_districts(df, b)
    assert len(out) == 2
    assert out["district"].isna().all()
    assert out["outside_polygon"].all()


def test_city_mismatch_between_row_and_polygon_is_flagged():
    b = load_boundaries(REAL_BOUNDARIES)
    df = rows_df([["Taipei City", "信義區", "x", 25.0143, 121.4638, "", ""]])  # Banqiao
    out = assign_districts(df, b)
    assert out.loc[0, "district_city"] == "New Taipei City"
    assert bool(out.loc[0, "city_mismatch"])


# ---- splitting ---------------------------------------------------------------

def district_rows(n, district="板橋區", city="New Taipei City", seed=0):
    rnd = random.Random(seed)
    rows = []
    for i in range(n):
        rows.append(
            {
                "point_id": f"X:{district}:{i:05d}",
                "city": city,
                "district": district,
                "source_area": district,
                "road": f"路{rnd.randint(0, 40)}",
                "dd_lat": 25.0 + rnd.random() / 10,
                "dd_long": 121.4 + rnd.random() / 10,
                "dms_lat": "",
                "dms_long": "",
            }
        )
    return pd.DataFrame(rows)


def test_district_up_to_limit_is_one_unnumbered_list():
    lists = split_into_lists(district_rows(500), max_rows=500)
    assert [l.name for l in lists] == ["新北市 板橋區"]
    assert lists[0].part is None and len(lists[0].rows) == 500


def test_district_over_limit_is_split_into_numbered_lists_of_at_most_limit():
    lists = split_into_lists(district_rows(1201), max_rows=500)
    assert [l.name for l in lists] == ["新北市 板橋區-1", "新北市 板橋區-2", "新北市 板橋區-3"]
    assert [len(l.rows) for l in lists] == [500, 500, 201]
    assert [l.part for l in lists] == [1, 2, 3]


def test_split_is_independent_of_input_order():
    df = district_rows(1100)
    a = split_into_lists(df, max_rows=500)
    b = split_into_lists(df.sample(frac=1, random_state=7), max_rows=500)
    assert [(l.name, list(l.rows["point_id"])) for l in a] == [
        (l.name, list(l.rows["point_id"])) for l in b
    ]


def test_adding_one_row_changes_only_lists_of_its_district():
    base = pd.concat(
        [
            district_rows(1100, "板橋區", seed=1),
            district_rows(600, "中和區", seed=2),
            district_rows(40, "金山區", seed=3),
        ],
        ignore_index=True,
    )
    extra = district_rows(1, "板橋區", seed=99)
    extra["point_id"] = "X:新增"
    after = pd.concat([base, extra], ignore_index=True)

    def snapshot(df):
        return {l.name: list(l.rows["point_id"]) for l in split_into_lists(df, max_rows=500)}

    s0, s1 = snapshot(base), snapshot(after)
    changed = {n for n in set(s0) | set(s1) if s0.get(n) != s1.get(n)}
    assert changed and all(n.startswith("新北市 板橋區") for n in changed)
    assert s0["新北市 中和區-1"] == s1["新北市 中和區-1"]
    assert s0["新北市 金山區"] == s1["新北市 金山區"]


def test_adding_one_row_shifts_at_most_one_row_across_each_boundary():
    """Fixed-capacity fill: a single insert moves one row per list boundary, not a reshuffle."""
    df = district_rows(1100, seed=5)
    extra = district_rows(1, seed=6)
    extra["point_id"] = "X:新增"
    s0 = {l.name: set(l.rows["point_id"]) for l in split_into_lists(df, 500)}
    s1 = {l.name: set(l.rows["point_id"]) for l in split_into_lists(pd.concat([df, extra]), 500)}
    for name in s0:
        assert len(s0[name] ^ s1[name]) <= 2


# ---- point ids -----------------------------------------------------------------

def test_point_id_format_and_rounding():
    assert point_id("Taipei City", 25.0339000001, 121.5645) == "TPE:25.033900,121.564500"
    assert point_id("New Taipei City", 25.022574, 121.479312) == "NTPC:25.022574,121.479312"


def test_colliding_points_keep_distinct_stable_ids():
    df = rows_df(
        [
            ["New Taipei City", "三重區", "水湳街", 25.017662, 121.427507, "", ""],
            ["New Taipei City", "三重區", "龍安路", 25.017662, 121.427507, "", ""],
            ["New Taipei City", "三重區", "龍安路", 25.017662, 121.427507, "", ""],  # exact duplicate
            ["New Taipei City", "三重區", "別處", 25.0, 121.4, "", ""],
        ]
    )
    ids = assign_point_ids(df)
    assert len(set(ids)) == 4
    assert ids.iloc[3] == "NTPC:25.000000,121.400000"
    # stable under row order
    ids2 = assign_point_ids(df.iloc[::-1].reset_index(drop=True))
    assert set(ids) == set(ids2)


def test_collision_ordinals_depend_on_content_not_input_order():
    """Rows identical up to the ordinal step: same coordinate text, same road, different
    area / dms. Shuffling the input must not move any id between rows."""
    rows = [
        ["New Taipei City", "三重區", "同路", 25.017662, 121.427507, "dmsA", "x"],
        ["New Taipei City", "蘆洲區", "同路", 25.017662, 121.427507, "dmsA", "x"],
        ["New Taipei City", "蘆洲區", "同路", 25.017662, 121.427507, "dmsB", "x"],
        ["New Taipei City", "蘆洲區", "同路", 25.017662, 121.427507, "dmsB", "x"],  # exact repeat
        ["New Taipei City", "三重區", "別路", 25.017662, 121.427507, "dmsA", "x"],
        ["New Taipei City", "三重區", "他路", 25.0, 121.4, "dmsA", "x"],
        # same rounded id, same area/road/dms, differ only beyond the 6th coordinate decimal
        ["New Taipei City", "板橋區", "細路", 25.0300001, 121.5, "dmsC", "x"],
        ["New Taipei City", "板橋區", "細路", 25.0300002, 121.5, "dmsC", "x"],
    ]
    df = rows_df(rows)

    def mapping(frame):
        ids = assign_point_ids(frame)
        return sorted((tuple(map(str, frame.loc[i, COLS])), ids[i]) for i in frame.index)

    expected = mapping(df)
    assert len({i for _, i in expected}) == len(rows)
    rnd = random.Random(3)
    for _ in range(25):
        order = list(df.index)
        rnd.shuffle(order)
        assert mapping(df.loc[order].reset_index(drop=True)) == expected


# ---- build + plan ---------------------------------------------------------------

def tiny_boundaries(tmp_path):
    def sq(x0, y0, x1, y1):
        return [[[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]]

    fc = {
        "type": "FeatureCollection",
        "metadata": {"source_url": "https://example.test", "downloaded": "2026-11-04"},
        "features": [
            {"type": "Feature", "properties": {"county": "臺北市", "city": "Taipei City", "town": "甲區", "towncode": "1"},
             "geometry": {"type": "Polygon", "coordinates": sq(121.0, 25.0, 121.1, 25.1)}},
            {"type": "Feature", "properties": {"county": "新北市", "city": "New Taipei City", "town": "乙區", "towncode": "2"},
             "geometry": {"type": "Polygon", "coordinates": sq(121.1, 25.0, 121.2, 25.1)}},
        ],
    }
    p = tmp_path / "b.geojson"
    p.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")
    return str(p)


def small_csv(tmp_path, n_a=3, n_b=4, extra=None):
    rows = [["Taipei City", "甲區", f"a{i}", 25.01 + i * 0.001, 121.01, "", ""] for i in range(n_a)]
    rows += [["New Taipei City", "", f"b{i}", 25.01 + i * 0.001, 121.11, "", ""] for i in range(n_b)]
    rows += extra or []
    p = tmp_path / "parking.csv"
    rows_df(rows).to_csv(p, index=False, encoding="utf-8-sig")
    return str(p)


def test_build_lists_writes_files_index_and_report(tmp_path):
    out = tmp_path / "lists"
    index = build_lists(small_csv(tmp_path), tiny_boundaries(tmp_path), str(out), max_rows=500)
    names = sorted(l["list"] for l in index["lists"])
    assert names == ["新北市 乙區", "臺北市 甲區"]
    on_disk = json.loads((out / "index.json").read_text(encoding="utf-8"))
    assert on_disk == index
    assert sum(l["rows"] for l in on_disk["lists"]) == 7
    for l in on_disk["lists"]:
        df = pd.read_csv(out / l["file"], encoding="utf-8-sig")
        assert len(df) == l["rows"] and df["point_id"].is_unique
    assert (out / "assignment_report.json").exists()


def test_build_lists_keeps_every_row_and_never_merges(tmp_path):
    dup = [["New Taipei City", "", "b0", 25.01, 121.11, "", ""]] * 2  # two rows on the same spot
    index = build_lists(small_csv(tmp_path, extra=dup), tiny_boundaries(tmp_path), str(tmp_path / "l"))
    assert sum(l["rows"] for l in index["lists"]) == 9


def test_build_lists_reports_outside_points_and_mismatches(tmp_path):
    extra = [
        ["New Taipei City", "", "sea", 26.0, 122.0, "", ""],                # outside
        ["Taipei City", "乙區", "wrong", 25.01, 121.02, "", ""],             # source says 乙區, polygon 甲區
    ]
    out = tmp_path / "l"
    build_lists(small_csv(tmp_path, extra=extra), tiny_boundaries(tmp_path), str(out))
    rep = json.loads((out / "assignment_report.json").read_text(encoding="utf-8"))
    assert len(rep["outside_polygon"]) == 1 and rep["outside_polygon"][0]["road"] == "sea"
    assert len(rep["area_mismatch"]) == 1 and rep["area_mismatch"][0]["road"] == "wrong"
    # the outside point is still in a list (unassigned), not dropped
    all_ids = set()
    index = json.loads((out / "index.json").read_text(encoding="utf-8"))
    for l in index["lists"]:
        all_ids |= set(pd.read_csv(out / l["file"], encoding="utf-8-sig")["road"])
    assert "sea" in all_ids


def test_stale_list_files_are_removed_on_rebuild(tmp_path):
    out = tmp_path / "l"
    build_lists(small_csv(tmp_path, n_a=3, n_b=4), tiny_boundaries(tmp_path), str(out))
    build_lists(small_csv(tmp_path, n_a=3, n_b=0), tiny_boundaries(tmp_path), str(out))
    assert sorted(p.name for p in out.glob("*.csv")) == ["臺北市_甲區.csv"]


def desired_from(tmp_path, **kw):
    out = tmp_path / "lists"
    build_lists(small_csv(tmp_path, **kw), tiny_boundaries(tmp_path), str(out))
    return str(out)


def test_plan_with_empty_state_is_full_initial_load(tmp_path):
    lists_dir = desired_from(tmp_path)
    plan = make_plan(lists_dir, {"version": 1, "lists": {}})
    assert plan["summary"]["add"] == 7 and plan["summary"]["remove"] == 0
    assert sorted(plan["summary"]["lists_new"]) == ["新北市 乙區", "臺北市 甲區"]
    assert all(len(p["add"]) == p["rows"] for p in plan["lists"].values())


def test_plan_add_and_remove_against_state(tmp_path):
    lists_dir = desired_from(tmp_path, n_a=3, n_b=4)
    full = make_plan(lists_dir, {"version": 1, "lists": {}})
    # pretend everything was synced, then change the data: drop one a-point, add one b-point
    state = {
        "version": 1,
        "lists": {
            name: {"points": {a["point_id"]: {"lat": a["lat"], "lon": a["lon"], "road": a["road"]} for a in p["add"]}}
            for name, p in full["lists"].items()
        },
    }
    lists_dir2 = desired_from(tmp_path, n_a=2, n_b=5)
    plan = make_plan(lists_dir2, state)
    s = plan["summary"]
    assert (s["add"], s["remove"], s["unchanged"]) == (1, 1, 6)
    assert [a["road"] for a in plan["lists"]["新北市 乙區"]["add"]] == ["b4"]
    assert [r["road"] for r in plan["lists"]["臺北市 甲區"]["remove"]] == ["a2"]
    assert s["lists_new"] == [] and s["lists_gone"] == []


def test_plan_removes_lists_that_no_longer_exist(tmp_path):
    lists_dir = desired_from(tmp_path, n_a=3, n_b=0)
    state = {"version": 1, "lists": {"新北市 乙區": {"points": {"NTPC:25.0,121.1": {"lat": 25.0, "lon": 121.1, "road": "z"}}}}}
    plan = make_plan(lists_dir, state)
    assert plan["summary"]["lists_gone"] == ["新北市 乙區"]
    assert plan["summary"]["remove"] == 1 and plan["summary"]["add"] == 3


def test_plan_moves_between_lists_are_remove_plus_add(tmp_path):
    lists_dir = desired_from(tmp_path, n_a=1, n_b=1)
    full = make_plan(lists_dir, {"version": 1, "lists": {}})
    pid_b = full["lists"]["新北市 乙區"]["add"][0]["point_id"]
    # state wrongly has the b-point in list 甲區
    state = {"version": 1, "lists": {"臺北市 甲區": {"points": {pid_b: {"lat": 1, "lon": 2, "road": "b0"}}}}}
    plan = make_plan(lists_dir, state)
    assert any(r["point_id"] == pid_b for r in plan["lists"]["臺北市 甲區"]["remove"])
    assert any(a["point_id"] == pid_b for a in plan["lists"]["新北市 乙區"]["add"])


def _synced_state(plan):
    return {
        "version": 1,
        "lists": {
            name: {"points": {a["point_id"]: {"lat": a["lat"], "lon": a["lon"], "road": a["road"],
                                              "payload_hash": a["payload_hash"]} for a in p["add"]}}
            for name, p in plan["lists"].items()
        },
    }


def test_changed_road_name_is_remove_plus_add_with_same_point_id(tmp_path):
    lists_dir = desired_from(tmp_path, n_a=2, n_b=2)
    state = _synced_state(make_plan(lists_dir, {"version": 1, "lists": {}}))
    assert make_plan(lists_dir, state)["summary"]["add"] == 0  # in sync -> no ops

    # the road of a1 is corrected in the source data
    csv = tmp_path / "parking.csv"
    df = pd.read_csv(csv, encoding="utf-8-sig", keep_default_na=False, dtype=str)
    df.loc[df["road"] == "a1", "road"] = "a1-corrected"
    df.to_csv(csv, index=False, encoding="utf-8-sig")
    build_lists(str(csv), tiny_boundaries(tmp_path), lists_dir)

    plan = make_plan(lists_dir, state)
    s = plan["summary"]
    assert (s["add"], s["remove"], s["changed"], s["unchanged"]) == (1, 1, 1, 3)
    lst = plan["lists"]["臺北市 甲區"]
    assert lst["add"][0]["road"] == "a1-corrected" and lst["add"][0]["reason"] == "changed"
    assert lst["remove"][0]["road"] == "a1" and lst["remove"][0]["reason"] == "changed"
    assert lst["add"][0]["point_id"] == lst["remove"][0]["point_id"]


def test_state_without_payload_hash_is_compared_via_stored_fields(tmp_path):
    lists_dir = desired_from(tmp_path, n_a=1, n_b=0)
    state = _synced_state(make_plan(lists_dir, {"version": 1, "lists": {}}))
    for l in state["lists"].values():
        for v in l["points"].values():
            del v["payload_hash"]  # older state file
    assert make_plan(lists_dir, state)["summary"]["add"] == 0


def test_float_noise_in_coordinates_is_not_a_change(tmp_path):
    lists_dir = desired_from(tmp_path, n_a=2, n_b=0)
    plan = make_plan(lists_dir, {"version": 1, "lists": {}})
    state = _synced_state(plan)
    for l in state["lists"].values():
        for v in l["points"].values():
            v["lat"] += 1e-13
    assert make_plan(lists_dir, state)["summary"]["add"] == 0


def test_assignment_report_keeps_exact_coordinates(tmp_path):
    extra = [["New Taipei City", "", "sea", 26.123456, 122.654321, "", ""]]
    out = tmp_path / "l"
    build_lists(small_csv(tmp_path, extra=extra), tiny_boundaries(tmp_path), str(out))
    rep = json.loads((out / "assignment_report.json").read_text(encoding="utf-8"))
    assert rep["outside_polygon"][0]["dd_lat"] == "26.123456"
    assert rep["outside_polygon"][0]["dd_long"] == "122.654321"


def test_assignment_report_rounds_only_border_distance(tmp_path):
    extra = [["Taipei City", "乙區", "wrong", 25.0123456, 121.0234567, "", ""]]
    out = tmp_path / "l"
    build_lists(small_csv(tmp_path, extra=extra), tiny_boundaries(tmp_path), str(out))
    rep = json.loads((out / "assignment_report.json").read_text(encoding="utf-8"))
    row = rep["area_mismatch"][0]
    assert row["dd_lat"] == "25.0123456" and row["dd_long"] == "121.0234567"
    assert row["border_m"] == round(row["border_m"], 1)


def test_committed_empty_sync_state_is_valid():
    state = json.load(open("data/sync_state.json", encoding="utf-8"))
    assert state["version"] == 1 and state["lists"] == {}


# ---- review loop 3 ----------------------------------------------------------------

def _snapshot(d):
    return {str(p.relative_to(d)): p.read_bytes() for p in sorted(d.rglob("*")) if p.is_file()}


def test_coordinate_text_variants_keep_stable_distinct_ids_under_shuffle(tmp_path):
    """'25.0400' and '25.04' are the same float but different source text; the id ordinal must
    follow the text, not the input position."""
    head = "city,area,road,dd_lat,dd_long,dms_lat,dms_long\n"
    rows = ["Taipei City,甲區,同路,25.0400,121.01,d,d",
            "Taipei City,甲區,同路,25.04,121.01,d,d",
            "Taipei City,甲區,同路,25.040,121.01,d,d"]
    bounds = tiny_boundaries(tmp_path)
    seen = []
    for k, order in enumerate([[0, 1, 2], [2, 1, 0], [1, 2, 0], [1, 0, 2]]):
        csv = tmp_path / f"in{k}.csv"
        csv.write_text(head + "\n".join(rows[i] for i in order) + "\n", encoding="utf-8")
        out = tmp_path / f"out{k}"
        build_lists(str(csv), bounds, str(out))
        df = pd.read_csv(next(out.glob("*.csv")), encoding="utf-8-sig", dtype=str, keep_default_na=False)
        assert df["point_id"].is_unique and len(df) == 3
        seen.append(dict(zip(df["dd_lat"], df["point_id"])))
    assert all(m == seen[0] for m in seen)


def test_failed_build_leaves_published_lists_untouched(tmp_path, monkeypatch):
    out = tmp_path / "lists"
    build_lists(small_csv(tmp_path, n_a=3, n_b=4), tiny_boundaries(tmp_path), str(out))
    before = _snapshot(out)
    calls = []
    real = pd.DataFrame.to_csv

    def flaky(self, *a, **k):
        calls.append(1)
        if len(calls) == 2:
            raise OSError("disk full")
        return real(self, *a, **k)

    monkeypatch.setattr(pd.DataFrame, "to_csv", flaky)
    with pytest.raises(OSError):
        build_lists(small_csv(tmp_path, n_a=5, n_b=0), tiny_boundaries(tmp_path), str(out))
    monkeypatch.undo()
    assert len(calls) >= 2
    assert _snapshot(out) == before
    assert sorted(p.name for p in tmp_path.iterdir() if p.name.startswith("lists")) == ["lists"]


def test_successful_rebuild_swaps_and_leaves_no_staging(tmp_path):
    out = tmp_path / "lists"
    build_lists(small_csv(tmp_path, n_a=3, n_b=4), tiny_boundaries(tmp_path), str(out))
    build_lists(small_csv(tmp_path, n_a=3, n_b=0), tiny_boundaries(tmp_path), str(out))
    assert sorted(p.name for p in tmp_path.iterdir() if p.name.startswith("lists")) == ["lists"]
    assert sorted(p.name for p in out.glob("*.csv")) == ["臺北市_甲區.csv"]
