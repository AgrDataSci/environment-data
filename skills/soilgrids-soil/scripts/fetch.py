# /// script
# requires-python = ">=3.12"
# dependencies = ["rasterio", "numpy", "pandas", "pyarrow"]
# ///
"""SoilGrids 2.0 soil properties for points, 250 m. One row per point, one column per property and depth.

    uv run fetch.py points.csv soil.parquet [--props clay,sand,soc] [--depths 0-5cm,5-15cm] [--stat mean]

points.csv needs columns id, lat, lon. Writes CSV if out ends in .csv, else parquet. Values are in conventional units (see SKILL.md)."""

import argparse
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import rasterio
from rasterio.env import Env
from rasterio.warp import transform

URL = "/vsicurl/https://files.isric.org/soilgrids/latest/data/{p}/{p}_{d}_{s}.vrt"
# stored integers / factor = conventional unit
FACTOR = {"bdod": 100, "cec": 10, "cfvo": 10, "clay": 10, "sand": 10, "silt": 10,
          "nitrogen": 100, "phh2o": 10, "soc": 10, "ocd": 10, "ocs": 10}
DEPTHS = ["0-5cm", "5-15cm", "15-30cm", "30-60cm", "60-100cm", "100-200cm"]
STATS = ["mean", "Q0.05", "Q0.5", "Q0.95"]
DEFAULT_PROPS = "clay,sand,silt,soc,nitrogen,phh2o,cec,bdod,cfvo"
DEFAULT_DEPTHS = "0-5cm,5-15cm,15-30cm"
GDAL = dict(GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", GDAL_HTTP_MAX_RETRY=3, GDAL_HTTP_RETRY_DELAY=2)


def layer(prop: str, depth: str, stat: str, lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    v = np.full(len(lat), np.nan, dtype="float32")
    ok = np.isfinite(lat) & np.isfinite(lon)  # blank coordinates stay NaN
    if not ok.any():
        return v
    # GDAL's block and /vsicurl caches already stop points sharing a strip from refetching it
    with Env(**GDAL), rasterio.open(URL.format(p=prop, d=depth, s=stat)) as r:
        xs, ys = transform("EPSG:4326", r.crs, lon[ok], lat[ok])
        v[ok] = [s[0] for s in r.sample(zip(xs, ys))]
        return np.where(v == r.nodata, np.nan, v) / FACTOR[prop]


def validate_points(points: pd.DataFrame) -> pd.DataFrame:
    """Unique ids so outputs join cleanly; WGS84 ranges catch swapped or projected coordinates."""
    points = points.reset_index(drop=True)
    if points.id.isna().any() or points.id.duplicated().any():
        raise ValueError("points need unique, non-empty ids")
    for col, limit in (("lat", 90), ("lon", 180)):
        if len(bad := points.id[points[col].abs() > limit]):
            raise ValueError(f"{col} outside ±{limit} for ids {bad.tolist()}")
    return points


def fetch(points: pd.DataFrame, props: list[str], depths: list[str], stat: str = "mean") -> pd.DataFrame:
    points = validate_points(points)
    if bad := (set(props) - set(FACTOR)) | (set(depths) - set(DEPTHS)) | ({stat} - set(STATS)):
        raise ValueError(f"unknown {sorted(bad)}; properties {', '.join(FACTOR)}; depths {', '.join(DEPTHS)}; stats {', '.join(STATS)}")
    # ocs (carbon stock) exists only for 0-30cm
    jobs = [(p, "0-30cm" if p == "ocs" else d) for p in props for d in depths]
    jobs = list(dict.fromkeys(jobs))
    lat, lon = points.lat.to_numpy(float), points.lon.to_numpy(float)
    with ThreadPoolExecutor(min(32, len(jobs))) as ex:  # each layer opens its own 6 MB VRT; network-bound
        cols = ex.map(lambda j: layer(*j, stat, lat, lon), jobs)
        out = {f"{p}_{d.replace('-', '_')}": c for (p, d), c in zip(jobs, cols)}
    return pd.DataFrame({"id": points.id, **out})


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("points"), p.add_argument("out")
    p.add_argument("--props", default=DEFAULT_PROPS, help=f"comma-separated from {', '.join(FACTOR)}")
    p.add_argument("--depths", default=DEFAULT_DEPTHS, help=f"comma-separated from {', '.join(DEPTHS)}")
    p.add_argument("--stat", default="mean", choices=STATS)
    a = p.parse_args()
    out = fetch(pd.read_csv(a.points, dtype={"id": str}), a.props.split(","), a.depths.split(","), a.stat)
    out.to_csv(a.out, index=False) if a.out.endswith(".csv") else out.to_parquet(a.out, index=False, compression="zstd")
    empty = out.drop(columns="id").isna().all(axis=1)
    print(f"{len(out)} points, {out.shape[1] - 1} layers ({a.stat}) -> {a.out}")
    if empty.any():
        ids = list(out.id[empty])
        print(f"{len(ids)} points with no soil data (water, urban, bare rock or off-grid): {ids[:20]}{' ...' if len(ids) > 20 else ''}")
