# /// script
# requires-python = ">=3.12"
# dependencies = ["xarray", "zarr>=3", "icechunk", "dask", "pandas", "pyarrow"]
# ///
"""Daily CHIRPS v3 rainfall for points. One row per point and day, mm.

    uv run fetch.py points.csv out.parquet --start 2020-01-01 [--end 2025-12-31]

points.csv needs columns id, lat, lon. end defaults to the latest day in the store."""

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


def fetch(points: pd.DataFrame, start: str, end: str | None = None) -> pd.DataFrame:
    ds = open_store()
    sel = dict(latitude=xr.DataArray(points.lat.values, dims="pt"), longitude=xr.DataArray(points.lon.values, dims="pt"))
    with dask.config.set(num_workers=THREADS):
        sub = ds.precipitation_surface.sel(time=slice(start, end)).sel(**sel, method="nearest").compute()
    df = (sub * 86400).to_dataframe(name="precip_mm").reset_index()  # kg m-2 s-1 -> mm/day
    df["id"] = points.id.values[df.pt]
    df["date"] = df.time.dt.date
    df = df.rename(columns={"latitude": "cell_lat", "longitude": "cell_lon"})
    return df[["id", "date", "precip_mm", "cell_lat", "cell_lon"]].astype({"precip_mm": "float32"})


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("points"), p.add_argument("out"), p.add_argument("--start", required=True), p.add_argument("--end")
    a = p.parse_args()
    out = fetch(pd.read_csv(a.points), a.start, a.end)
    out.to_parquet(a.out, index=False, compression="zstd")
    sea = out.groupby("id").precip_mm.apply(lambda s: s.isna().all())
    print(f"{out.id.nunique()} points, {out.date.min()} to {out.date.max()}, {len(out):,} rows -> {a.out}")
    if sea.any():
        print(f"all-NaN (sea or off-grid), move inland: {list(sea[sea].index)}")
