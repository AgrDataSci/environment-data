# /// script
# requires-python = ">=3.12"
# dependencies = ["rasterio", "numpy", "pandas", "pyarrow"]
# ///
"""Elevation in metres for points from the Copernicus GLO-90 DEM. One row per point.

    uv run fetch.py points.csv elevation.parquet

points.csv needs columns id, lat, lon."""

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


def fetch(points: pd.DataFrame) -> pd.DataFrame:
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
                    out[idx] = [v[0] for v in dem.sample(xy)]
            except RasterioIOError:  # no tile over open ocean
                pass
    return pd.DataFrame({"id": points.id, "elevation_m": out.astype("float32")})


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("points"), p.add_argument("out")
    a = p.parse_args()
    out = fetch(pd.read_csv(a.points))
    out.to_parquet(a.out, index=False)
    ok = out.elevation_m.notna()
    print(f"{ok.sum()} of {len(out)} points, {out.elevation_m.min():.0f} to {out.elevation_m.max():.0f} m -> {a.out}")
    if (~ok).any():
        print(f"no elevation (open water or missing coordinates): {list(out.id[~ok])}")
