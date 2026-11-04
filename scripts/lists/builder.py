"""
Build per-district list files from data/parking_locations.csv.

Every CSV row stays its own point (nothing is merged). One list per
city + district; a district with more than max_rows rows is split into numbered
lists of at most max_rows rows each (Google Maps saved lists are unofficially
capped around 500 places).

Determinism: rows are sorted by (road, lat, long, point_id) before being cut
into consecutive chunks of max_rows, so the result does not depend on input
order, and adding one row changes only the lists of its own district (and shifts
at most one row across each chunk boundary there).
"""

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .districts import assign_districts, load_boundaries
from .sync_plan import assign_point_ids

CITY_ZH = {"Taipei City": "臺北市", "New Taipei City": "新北市"}
CITY_ORDER = {"Taipei City": 0, "New Taipei City": 1}
UNASSIGNED = "未分區"
DEFAULT_MAX_ROWS = 500
OUT_COLUMNS = ["point_id", "city", "district", "source_area", "road",
               "dd_lat", "dd_long", "dms_lat", "dms_long"]
SORT_KEYS = ["road", "_lat", "_lon", "point_id"]


@dataclass
class ListSpec:
    name: str
    city: str
    district: str
    part: Optional[int]
    rows: pd.DataFrame

    @property
    def file(self) -> str:
        suffix = f"-{self.part}" if self.part else ""
        return f"{CITY_ZH.get(self.city, self.city)}_{self.district}{suffix}.csv"


def split_into_lists(df: pd.DataFrame, max_rows: int = DEFAULT_MAX_ROWS) -> List[ListSpec]:
    """df needs point_id, city, district, road, dd_lat, dd_long."""
    df = df.copy()
    df["_lat"] = df["dd_lat"].astype(float)
    df["_lon"] = df["dd_long"].astype(float)
    specs: List[ListSpec] = []
    for (city, district), grp in df.groupby(["city", "district"], sort=False):
        grp = grp.sort_values(SORT_KEYS, kind="mergesort").drop(columns=["_lat", "_lon"])
        base = f"{CITY_ZH.get(city, city)} {district}"
        if len(grp) <= max_rows:
            specs.append(ListSpec(base, city, district, None, grp))
        else:
            for k, start in enumerate(range(0, len(grp), max_rows), start=1):
                specs.append(ListSpec(f"{base}-{k}", city, district, k, grp.iloc[start:start + max_rows]))
    specs.sort(key=lambda s: (CITY_ORDER.get(s.city, 99), s.district, s.part or 0))
    return specs


def _report_rows(df: pd.DataFrame, extra: List[str]) -> List[Dict[str, Any]]:
    cols = ["point_id", "city", "source_area", "road", "dd_lat", "dd_long"] + extra
    out = []
    for r in df[cols].to_dict("records"):
        row = {}
        for k, v in r.items():
            if isinstance(v, float) and pd.isna(v):
                v = None
            elif k == "border_m" and v is not None:
                v = round(v, 1)  # only the triage distance is rounded; coordinates stay exact
            row[k] = v
        out.append(row)
    return out


def _validate_generation(gen: Path, index: Dict[str, Any], input_rows: int) -> None:
    """Every file named in the index exists with the stated rows, and the totals match the input."""
    for need in ("index.json", "assignment_report.json"):
        if not (gen / need).is_file():
            raise RuntimeError(f"list generation incomplete: {need} missing")
    total = 0
    for entry in index["lists"]:
        f = gen / entry["file"]
        if not f.is_file():
            raise RuntimeError(f"list generation incomplete: {entry['file']} missing")
        n = len(pd.read_csv(f, encoding="utf-8-sig", dtype=str, keep_default_na=False))
        if n != entry["rows"]:
            raise RuntimeError(f"{entry['file']} has {n} rows on disk, index says {entry['rows']}")
        total += n
    if total != input_rows or index["total_rows"] != input_rows:
        raise RuntimeError(f"list rows {total} != input rows {input_rows}")
    extra = {p.name for p in gen.glob("*.csv")} - {e["file"] for e in index["lists"]}
    if extra:
        raise RuntimeError(f"list generation has files not in the index: {sorted(extra)}")


def build_lists(csv_path: str, boundaries_path: str, out_dir: str,
                max_rows: int = DEFAULT_MAX_ROWS) -> Dict[str, Any]:
    """Write data/lists/*.csv, index.json and assignment_report.json; return the index."""
    raw = pd.read_csv(csv_path, encoding="utf-8-sig", keep_default_na=False, dtype=str)
    # dd_lat/dd_long stay SOURCE TEXT: ids and sort ties use the exact text; assign_districts
    # and point_id convert to float themselves, for geometry and the 6-decimal id only.
    df = raw.copy()
    b = load_boundaries(boundaries_path)
    df = assign_districts(df, b)
    df["point_id"] = assign_point_ids(df)
    df["source_area"] = df["area"]
    df["list_district"] = df["district"].where(~df["outside_polygon"], UNASSIGNED)

    to_split = df.assign(district=df["list_district"])
    specs = split_into_lists(to_split, max_rows)

    out = Path(out_dir)
    staging = out.parent / f"{out.name}.tmp-{os.getpid()}"
    old = out.parent / f"{out.name}.old-{os.getpid()}"
    for leftover in (staging, old):
        shutil.rmtree(leftover, ignore_errors=True)
    staging.mkdir(parents=True)
    try:
        index_lists = []
        for s in specs:
            s.rows[OUT_COLUMNS].to_csv(staging / s.file, index=False, encoding="utf-8-sig")
            index_lists.append({"list": s.name, "file": s.file, "city": s.city,
                                "district": s.district, "part": s.part, "rows": len(s.rows)})

        meta = b.metadata
        index = {
            "max_rows_per_list": max_rows,
            "total_rows": int(len(df)),
            "source_csv": str(csv_path).replace("\\", "/"),
            "boundary_source": {k: meta.get(k) for k in ("source_dataset", "source_url", "source_version", "downloaded")},
            "lists": index_lists,
        }
        (staging / "index.json").write_text(
            json.dumps(index, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )

        report = {
            "outside_polygon": _report_rows(df[df["outside_polygon"]], []),
            "city_mismatch": _report_rows(df[df["city_mismatch"]], ["district_city", "district", "border_m"]),
            "area_mismatch": _report_rows(df[df["area_mismatch"]], ["district", "border_m"]),
            "counts": {
                "rows": int(len(df)),
                "outside_polygon": int(df["outside_polygon"].sum()),
                "city_mismatch": int(df["city_mismatch"].sum()),
                "area_mismatch_by_city": {k: int(v) for k, v in df[df["area_mismatch"]].groupby("city").size().items()},
                "rows_with_source_area_by_city": {k: int(v) for k, v in df[df["source_area"] != ""].groupby("city").size().items()},
            },
        }
        (staging / "assignment_report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

        _validate_generation(staging, index, len(df))
        # swap: the published directory is replaced by one rename, never edited in place
        if out.exists():
            out.rename(old)
        try:
            staging.rename(out)
        except OSError:
            if old.exists():
                old.rename(out)
            raise
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    shutil.rmtree(old, ignore_errors=True)
    return index
