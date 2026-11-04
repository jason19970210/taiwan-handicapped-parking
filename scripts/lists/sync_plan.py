"""
Stable point ids and the add/remove plan between data/lists and sync_state.json.

sync_state.json records what was last written to Google Maps, per list, keyed by
point id. This module only reads state and lists; it never writes to Google.
"""

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict

import pandas as pd

CITY_CODE = {"Taipei City": "TPE", "New Taipei City": "NTPC"}
EMPTY_STATE = {"version": 1, "lists": {}}


def _city_code(city: str) -> str:
    return CITY_CODE.get(city) or re.sub(r"[^A-Za-z0-9]+", "_", city).strip("_").upper() or "UNK"


def point_id(city: str, lat: float, lon: float) -> str:
    """City code + coordinates rounded to 6 decimals (~0.1 m)."""
    return f"{_city_code(city)}:{float(lat):.6f},{float(lon):.6f}"


def assign_point_ids(df: pd.DataFrame) -> pd.Series:
    """
    Unique, order-independent id per row. Rows are never merged: when several
    rows share one coordinate (the source has such rows with different
    road/area), the base id gets a short hash of the road, and exact repeats an
    ordinal, so no two rows ever collide.
    """
    base = pd.Series(
        [point_id(c, la, lo) for c, la, lo in zip(df["city"], df["dd_lat"], df["dd_long"])],
        index=df.index,
    )
    ids = base.copy()
    dup = base.duplicated(keep=False)
    if dup.any():
        roads = df["road"].fillna("").astype(str)
        for i in df.index[dup]:
            ids[i] = f"{base[i]}#{hashlib.md5(roads[i].encode('utf-8')).hexdigest()[:6]}"
        again = ids.duplicated(keep=False)
        if again.any():
            # Sort on every remaining field (exact coordinate text included) so the
            # ordinal depends only on row content, never on input order.
            keys = ["_id"] + [c for c in ("area", "road", "dd_lat", "dd_long", "dms_lat", "dms_long")
                              if c in df.columns]
            sortable = df[again].assign(_id=ids[again])
            for c in keys[1:]:
                sortable[c] = sortable[c].astype(str)
            order = sortable.sort_values(keys, kind="mergesort")
            for _id, grp in order.groupby("_id", sort=False):
                for n, i in enumerate(grp.index, start=1):
                    ids[i] = f"{_id}.{n}"
    return ids


def payload_hash(road, lat, lon) -> str:
    """Hash of what is written to Google for a point (name from road, position at 6 decimals).
    A changed hash for an existing point id means the place must be rewritten."""
    text = f"{str(road or '').strip()}|{float(lat):.6f}|{float(lon):.6f}"
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]


def load_state(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"version": 1, "lists": {}}
    state = json.loads(p.read_text(encoding="utf-8"))
    state.setdefault("lists", {})
    return state


def _point(row) -> Dict[str, Any]:
    return {
        "point_id": row["point_id"],
        "lat": float(row["dd_lat"]),
        "lon": float(row["dd_long"]),
        "road": row["road"] if isinstance(row["road"], str) else "",
        "payload_hash": payload_hash(row["road"], row["dd_lat"], row["dd_long"]),
        "dms_lat": row.get("dms_lat", ""),
        "dms_long": row.get("dms_long", ""),
    }


def _stored_hash(v: Dict[str, Any]) -> str:
    """payload_hash recorded in the state, or recomputed from the stored fields for older state."""
    return v.get("payload_hash") or payload_hash(v.get("road", ""), v.get("lat", 0), v.get("lon", 0))


def make_plan(lists_dir: str, state: Dict[str, Any]) -> Dict[str, Any]:
    """Compare data/lists with the recorded sync state and return the add/remove plan."""
    lists_dir = Path(lists_dir)
    index = json.loads((lists_dir / "index.json").read_text(encoding="utf-8"))
    state_lists = state.get("lists", {})

    plan_lists: Dict[str, Any] = {}
    totals = {"add": 0, "remove": 0, "unchanged": 0, "changed": 0}
    desired_names = set()

    for entry in index["lists"]:
        name = entry["list"]
        desired_names.add(name)
        df = pd.read_csv(lists_dir / entry["file"], encoding="utf-8-sig", keep_default_na=False, dtype=str)
        desired = {r["point_id"]: r for r in df.to_dict("records")}
        have = state_lists.get(name, {}).get("points", {})
        new_ids = [i for i in sorted(desired) if i not in have]
        changed_ids = {
            i for i in sorted(desired)
            if i in have and _stored_hash(have[i]) != _point(desired[i])["payload_hash"]
        }
        # A changed payload is an explicit remove + add of the same point id, marked "changed".
        add = [_point(desired[i]) for i in sorted(set(new_ids) | changed_ids)]
        for a_ in add:
            if a_["point_id"] in changed_ids:
                a_["reason"] = "changed"
        remove = [
            {"point_id": i, "lat": have[i].get("lat"), "lon": have[i].get("lon"),
             "road": have[i].get("road", ""),
             **({"reason": "changed"} if i in changed_ids else {})}
            for i in sorted(have)
            if i not in desired or i in changed_ids
        ]
        unchanged = len(desired) - len(new_ids) - len(changed_ids)
        plan_lists[name] = {
            "city": entry["city"],
            "district": entry["district"],
            "part": entry["part"],
            "rows": len(desired),
            "add": add,
            "remove": remove,
            "unchanged": unchanged,
            "changed": len(changed_ids),
        }
        totals["changed"] += len(changed_ids)
        totals["add"] += len(add)
        totals["remove"] += len(remove)
        totals["unchanged"] += unchanged

    lists_gone = sorted(n for n in state_lists if n not in desired_names)
    for name in lists_gone:
        have = state_lists[name].get("points", {})
        remove = [
            {"point_id": i, "lat": v.get("lat"), "lon": v.get("lon"), "road": v.get("road", "")}
            for i, v in sorted(have.items())
        ]
        plan_lists[name] = {"city": None, "district": None, "part": None, "rows": 0,
                            "add": [], "remove": remove, "unchanged": 0, "changed": 0}
        totals["remove"] += len(remove)

    return {
        "summary": {
            "lists": len(desired_names),
            **totals,
            "lists_new": sorted(n for n in desired_names if n not in state_lists),
            "lists_gone": lists_gone,
        },
        "lists": plan_lists,
    }
