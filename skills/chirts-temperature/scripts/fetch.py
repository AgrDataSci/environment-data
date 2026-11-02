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


def fetch(points: pd.DataFrame, start: str, end: str | None = None) -> pd.DataFrame:
    ds = xr.open_zarr(STORE, consolidated=True, chunks={})
    sel = dict(lat=xr.DataArray(points.lat.values, dims="pt"), lon=xr.DataArray(points.lon.values, dims="pt"))
    with dask.config.set(num_workers=THREADS):
        sub = ds[["Tmax", "Tmin"]].sel(time=slice(start, end)).sel(**sel, method="nearest").compute()
    sub = sub.where(sub <= TMAX_OK)
    df = sub.to_dataframe().reset_index().rename(columns={"Tmax": "tmax_c", "Tmin": "tmin_c", "lat": "cell_lat", "lon": "cell_lon"})
    df["id"] = points.id.values[df.pt]
    df["date"] = df.time.dt.date
    return df[["id", "date", "tmax_c", "tmin_c", "cell_lat", "cell_lon"]].astype({"tmax_c": "float32", "tmin_c": "float32"})


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("points"), p.add_argument("out"), p.add_argument("--start", required=True), p.add_argument("--end")
    a = p.parse_args()
    out = fetch(pd.read_csv(a.points), a.start, a.end)
    out.to_parquet(a.out, index=False, compression="zstd")
    sea = out.groupby("id").tmax_c.apply(lambda s: s.isna().all())
    print(f"{out.id.nunique()} points, {out.date.min()} to {out.date.max()}, {len(out):,} rows -> {a.out}")
    if sea.any():
        print(f"all-NaN (sea, lake or off-grid), move inland: {list(sea[sea].index)}")
