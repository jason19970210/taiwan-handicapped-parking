"""
District assignment by point-in-polygon against the official MOI township
boundaries (see scripts/build_boundaries.py for the source and how the polygon
file was made).
"""

import json
from dataclasses import dataclass
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import shapely
from shapely.geometry import shape

# Approximate metres per degree around 25N, only used to give triage distances
_M_PER_DEG = 106_000.0


@dataclass
class Boundaries:
    geoms: np.ndarray            # shapely polygons, sorted by towncode
    props: List[Dict[str, Any]]  # county, city, town, towncode
    metadata: Dict[str, Any]
    tree: shapely.STRtree

    @property
    def polygons(self):
        return self.geoms


def load_boundaries(path: str) -> Boundaries:
    with open(path, encoding="utf-8") as f:
        fc = json.load(f)
    feats = sorted(fc["features"], key=lambda x: x["properties"]["towncode"])
    geoms = np.array([shape(f["geometry"]) for f in feats], dtype=object)
    props = [f["properties"] for f in feats]
    return Boundaries(geoms, props, fc.get("metadata", {}), shapely.STRtree(geoms))


def assign_districts(df: pd.DataFrame, b: Boundaries) -> pd.DataFrame:
    """
    Add district columns to a copy of df (needs city, area, dd_lat, dd_long):

      district        polygon district name, or None if the point is in no polygon
      district_code   MOI TOWNCODE
      district_city   city the polygon belongs to ("Taipei City"/"New Taipei City")
      outside_polygon True if no polygon contains the point
      city_mismatch   row's city differs from the polygon's city
      area_mismatch   row's own `area` is non-empty and differs from the polygon district
      border_m        approx. distance (m) from the point to the assigned polygon's edge

    A point exactly on a shared edge matches two polygons; the lowest TOWNCODE
    wins, so the result is deterministic. Rows are never dropped.
    """
    out = df.copy()
    pts = shapely.points(out["dd_long"].astype(float).values, out["dd_lat"].astype(float).values)
    pi, gi = b.tree.query(pts, predicate="intersects")
    best = np.full(len(out), -1, dtype=int)
    # iterate in reverse so that, for a point on a shared edge, the lowest polygon index wins
    for p, g in sorted(zip(pi.tolist(), gi.tolist()), reverse=True):
        best[p] = g

    town = np.array([p["town"] for p in b.props], dtype=object)
    code = np.array([p["towncode"] for p in b.props], dtype=object)
    city = np.array([p["city"] for p in b.props], dtype=object)
    inside = best >= 0
    safe = np.where(inside, best, 0)
    out["district"] = np.where(inside, town[safe], None)
    out["district_code"] = np.where(inside, code[safe], None)
    out["district_city"] = np.where(inside, city[safe], None)
    out["outside_polygon"] = ~inside
    out["city_mismatch"] = inside & (out["city"].values != out["district_city"].values)
    area = out["area"].fillna("").astype(str).values
    out["area_mismatch"] = inside & (area != "") & (area != out["district"].fillna("").values)

    border = np.full(len(out), np.nan)
    if inside.any():
        edges = shapely.boundary(b.geoms[best[inside]])
        border[inside] = shapely.distance(pts[inside], edges) * _M_PER_DEG
    out["border_m"] = border
    return out
