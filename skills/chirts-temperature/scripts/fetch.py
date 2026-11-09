# /// script
# requires-python = ">=3.12"
# dependencies = ["xarray", "zarr>=3", "dask", "fsspec", "aiohttp", "pandas", "pyarrow"]
# ///
"""Daily CHIRTS-ERA5 Tmax and Tmin for points. One row per point and day, degrees C.

    uv run fetch.py points.csv out.parquet --start 2020-01-01 [--end 2025-12-31]

points.csv needs columns id, lat, lon. end defaults to the latest day in the store."""

import argparse
import dask
import pandas as pd
import xarray as xr

STORE = "https://huggingface.co/datasets/aaguilar90/chirts-era5-daily/resolve/main/chirts-era5-daily.zarr"
THREADS = 32  # reads wait on the network, not the CPU
TMAX_OK = 60  # a few source pixels hold impossible values; mask above this


def validate_points(points: pd.DataFrame) -> pd.DataFrame:
    """Unique ids so outputs join cleanly; WGS84 ranges catch swapped or projected coordinates."""
    points = points.reset_index(drop=True)
    if points.id.isna().any() or points.id.duplicated().any():
        raise ValueError("points need unique, non-empty ids")
    for col, limit in (("lat", 90), ("lon", 180)):
        if len(bad := points.id[points[col].abs() > limit]):
            raise ValueError(f"{col} outside ±{limit} for ids {bad.tolist()}")
    return points


def select_daily(ds, points, start, end, lat, lon):
    """Nearest cell per point. Off-grid points stay NaN; nearest alone would snap them to the edge cell."""
    first, last = pd.to_datetime(ds.time.values[[0, -1]])
    start, end = pd.Timestamp(start), pd.Timestamp(end or last)
    if not first <= start <= end <= last:  # else the store silently returns a shorter series
        raise ValueError(f"dates must be ordered and within {first.date()} to {last.date()}")
    on_grid = points.lat.notna() & points.lon.notna()
    for col, name in (("lat", lat), ("lon", lon)):
        axis = ds[name].values
        half = abs(axis[1] - axis[0]) / 2
        on_grid &= points[col].between(axis.min() - half, axis.max() + half)
    pts = points[on_grid]
    sel = {name: xr.DataArray(pts[col].values, dims="pt", coords={"pt": pts.index}) for col, name in (("lat", lat), ("lon", lon))}
    with dask.config.set(num_workers=THREADS):
        sub = ds.sel(time=slice(start, end)).sel(**sel, method="nearest").compute()
    return sub.reindex(pt=points.index)


def fetch(points: pd.DataFrame, start: str, end: str | None = None) -> pd.DataFrame:
    points = validate_points(points)
    ds = xr.open_zarr(STORE, consolidated=True, chunks={})
    sub = select_daily(ds[["Tmax", "Tmin"]], points, start, end, "lat", "lon")
    sub = sub.where(sub <= TMAX_OK)
    df = sub.to_dataframe().reset_index().rename(columns={"Tmax": "tmax_c", "Tmin": "tmin_c", "lat": "cell_lat", "lon": "cell_lon"})
    df["id"] = points.id.values[df.pt]
    df["date"] = df.time.dt.date
    return df[["id", "date", "tmax_c", "tmin_c", "cell_lat", "cell_lon"]].astype({"tmax_c": "float32", "tmin_c": "float32"})


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("points"), p.add_argument("out"), p.add_argument("--start", required=True), p.add_argument("--end")
    a = p.parse_args()
    out = fetch(pd.read_csv(a.points, dtype={"id": str}), a.start, a.end)
    out.to_parquet(a.out, index=False, compression="zstd")
    sea = out[["tmax_c", "tmin_c"]].isna().all(axis=1).groupby(out.id).all()
    print(f"{out.id.nunique()} points, {out.date.min()} to {out.date.max()}, {len(out):,} rows -> {a.out}")
    if sea.any():
        print(f"all-NaN (missing coordinates, outside coverage or source nodata): {list(sea[sea].index)}")
