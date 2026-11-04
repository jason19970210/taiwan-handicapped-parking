"""
One-off tool: build data/boundaries/tw_districts_tpe_ntpc.geojson from the
official MOI/NLSC township boundary shapefile.

Source (public open data, no login):
  data.gov.tw dataset 7441, 鄉鎮市區界線(TWD97經緯度), provider: 國土測繪中心 (NLSC, MOI)
  https://data.gov.tw/dataset/7441
  https://www.tgos.tw/tgos/VirtualDir/Product/3fe61d4a-ca23-4f45-8aca-4a536f40f290/鄉(鎮、市、區)界線1140318.zip

Usage (extract the zip into a NEW EMPTY directory first, run with -I):
  .venv/bin/python -I scripts/build_boundaries.py \\
      --shp /path/to/extracted/TOWN_MOI_1140318.shp \\
      --zip-sha256 <sha256 of the downloaded zip> \\
      --out data/boundaries/tw_districts_tpe_ntpc.geojson

Only 臺北市 and 新北市 are kept. All polygons are simplified together with
shapely.coverage_simplify (shared borders stay shared, so no gaps or overlaps
appear between neighbouring districts) and snapped to a 1e-5 degree grid (~1 m).
Coordinates stay in the published geographic CRS (TWD97 lon/lat), which agrees
with WGS84 to well under a metre for this purpose.
"""

import argparse
import json
from datetime import date

import geopandas as gpd
import shapely
from shapely.geometry import mapping

KEEP_COUNTIES = {"臺北市": "Taipei City", "新北市": "New Taipei City"}
DEFAULT_TOLERANCE = 0.00005  # degrees, ~5 m


def build(shp_path: str, tolerance: float, zip_sha256: str = "", version: str = ""):
    gdf = gpd.read_file(shp_path, encoding="utf-8")
    gdf = gdf[gdf["COUNTYNAME"].isin(KEEP_COUNTIES)].sort_values("TOWNCODE").reset_index(drop=True)
    if len(gdf) != 41:
        raise SystemExit(f"expected 41 districts (12 Taipei + 29 New Taipei), got {len(gdf)}")
    if not shapely.coverage_is_valid(gdf.geometry.values):
        raise SystemExit("source polygons do not form a valid coverage")
    simplified = shapely.coverage_simplify(gdf.geometry.values, tolerance)
    simplified = shapely.set_precision(simplified, 1e-5)
    features = []
    for row, geom in zip(gdf.itertuples(), simplified):
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "county": row.COUNTYNAME,
                    "city": KEEP_COUNTIES[row.COUNTYNAME],
                    "town": row.TOWNNAME,
                    "towncode": row.TOWNCODE,
                },
                "geometry": mapping(geom),
            }
        )
    return {
        "type": "FeatureCollection",
        "metadata": {
            "description": "Taipei City + New Taipei City district polygons for point-in-polygon assignment",
            "source_dataset": "鄉鎮市區界線(TWD97經緯度), data.gov.tw dataset 7441, provider 國土測繪中心 (MOI/NLSC)",
            "source_page": "https://data.gov.tw/dataset/7441",
            "source_url": "https://www.tgos.tw/tgos/VirtualDir/Product/3fe61d4a-ca23-4f45-8aca-4a536f40f290/鄉(鎮、市、區)界線1140318.zip",
            "source_version": version or "1140318 (ROC 114-03-18, i.e. 2025-03-18)",
            "source_zip_sha256": zip_sha256,
            "downloaded": date.today().isoformat(),
            "crs": "TWD97 geographic lon/lat (published as-is; equals WGS84 within ~1 m)",
            "simplification": f"shapely.coverage_simplify tolerance={tolerance} deg, snapped to 1e-5 deg",
            "polygons": len(features),
        },
        "features": features,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--shp", required=True)
    ap.add_argument("--out", default="data/boundaries/tw_districts_tpe_ntpc.geojson")
    ap.add_argument("--tolerance", type=float, default=DEFAULT_TOLERANCE)
    ap.add_argument("--zip-sha256", default="")
    ap.add_argument("--version", default="")
    args = ap.parse_args()
    fc = build(args.shp, args.tolerance, args.zip_sha256, args.version)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, separators=(",", ":"))
    import os

    print(f"wrote {args.out}: {os.path.getsize(args.out)} bytes, {len(fc['features'])} polygons")


if __name__ == "__main__":
    main()
