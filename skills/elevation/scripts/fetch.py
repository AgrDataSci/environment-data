# /// script
# requires-python = ">=3.12"
# dependencies = ["rasterio", "numpy", "pandas", "pyarrow"]
# ///
"""Elevation in metres for points from the Copernicus GLO-90 DEM. One row per point.

    uv run fetch.py points.csv elevation.parquet

points.csv needs columns id, lat, lon. Writes CSV if out ends in .csv, else parquet."""

import argparse
import math
from collections import defaultdict

import numpy as np
import pandas as pd
import rasterio
from rasterio.env import Env
from rasterio.errors import RasterioIOError

TILE = (
    "https://copernicus-dem-90m.s3.amazonaws.com/"
    "Copernicus_DSM_COG_30_{t}_DEM/Copernicus_DSM_COG_30_{t}_DEM.tif"
)


def tile_url(lat: float, lon: float) -> str:
    """Tiles are 1 degree, named by their south-west corner: S02_00_E029_00 covers 2-1 S, 29-30 E."""
    y, x = math.floor(lat), math.floor(lon)
    t = f"{'N' if y >= 0 else 'S'}{abs(y):02d}_00_{'E' if x >= 0 else 'W'}{abs(x):03d}_00"
    return TILE.format(t=t)


def validate_points(points: pd.DataFrame) -> pd.DataFrame:
    """Unique ids so outputs join cleanly; WGS84 ranges catch swapped or projected coordinates."""
    points = points.reset_index(drop=True)
    if points.id.isna().any() or points.id.duplicated().any():
        raise ValueError("points need unique, non-empty ids")
    for col, limit in (("lat", 90), ("lon", 180)):
        if len(bad := points.id[points[col].abs() > limit]):
            raise ValueError(f"{col} outside ±{limit} for ids {bad.tolist()}")
    return points


def fetch(points: pd.DataFrame) -> pd.DataFrame:
    points = validate_points(points)
    out = np.full(len(points), np.nan)
    by_tile = defaultdict(list)
    for i, (lat, lon) in enumerate(zip(points.lat, points.lon)):
        if np.isfinite(lat) and np.isfinite(lon):
            by_tile[tile_url(lat, lon)].append(i)
    with Env(GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR"):  # else GDAL lists the bucket on every open
        for url, idx in by_tile.items():
            try:
                with rasterio.open(url) as dem:
                    xy = zip(points.lon.values[idx], points.lat.values[idx])
                    out[idx] = [v.filled(np.nan)[0] for v in dem.sample(xy, masked=True)]
            except RasterioIOError as exc:
                # A missing ocean tile is expected; a failed connection is not nodata.
                if "404" not in str(exc):
                    raise
    return pd.DataFrame({"id": points.id, "elevation_m": out.astype("float32")})


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("points"), p.add_argument("out")
    a = p.parse_args()
    out = fetch(pd.read_csv(a.points, dtype={"id": str}))
    out.to_csv(a.out, index=False) if a.out.endswith(".csv") else out.to_parquet(a.out, index=False, compression="zstd")
    ok = out.elevation_m.notna()
    print(f"{ok.sum()} of {len(out)} points, {out.elevation_m.min():.0f} to {out.elevation_m.max():.0f} m -> {a.out}")
    if (~ok).any():
        print(f"no elevation (missing coordinates, missing tile or source nodata): {list(out.id[~ok])}")
