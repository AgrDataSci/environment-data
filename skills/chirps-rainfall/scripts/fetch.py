# /// script
# requires-python = ">=3.12"
# dependencies = ["xarray", "zarr>=3", "icechunk", "dask", "pandas", "pyarrow"]
# ///
"""Daily CHIRPS v3 rainfall for points. One row per point and day, mm.

    uv run fetch.py points.csv out.parquet --start 2020-01-01 [--end 2025-12-31]

points.csv needs columns id, lat, lon. Writes CSV if out ends in .csv, else parquet. end defaults to the latest day in the store."""

import argparse
import dask
import icechunk
import pandas as pd
import xarray as xr

STORE = ("dynamical-ucsb-chc-chirps", "ucsb-chc-chirps-analysis-final/v0.1.0.icechunk")
THREADS = 32  # reads wait on the network, not the CPU


def open_store() -> xr.Dataset:
    storage = icechunk.s3_storage(bucket=STORE[0], prefix=STORE[1], region="us-west-2", anonymous=True)
    session = icechunk.Repository.open(storage).readonly_session("main")
    return xr.open_zarr(session.store, consolidated=False, chunks={})


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
    ds = open_store()
    sub = select_daily(ds.precipitation_surface, points, start, end, "latitude", "longitude")
    df = (sub * 86400).to_dataframe(name="precip_mm").reset_index()  # kg m-2 s-1 -> mm/day
    df["id"] = points.id.values[df.pt]
    df["date"] = df.time.dt.date
    df = df.rename(columns={"latitude": "cell_lat", "longitude": "cell_lon"})
    return df[["id", "date", "precip_mm", "cell_lat", "cell_lon"]].astype({"precip_mm": "float32"})


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("points"), p.add_argument("out"), p.add_argument("--start", required=True), p.add_argument("--end")
    a = p.parse_args()
    out = fetch(pd.read_csv(a.points, dtype={"id": str}), a.start, a.end)
    out.to_csv(a.out, index=False) if a.out.endswith(".csv") else out.to_parquet(a.out, index=False, compression="zstd")
    sea = out.groupby("id").precip_mm.apply(lambda s: s.isna().all())
    print(f"{out.id.nunique()} points, {out.date.min()} to {out.date.max()}, {len(out):,} rows -> {a.out}")
    if sea.any():
        print(f"all-NaN (missing coordinates, outside coverage or source nodata): {list(sea[sea].index)}")
