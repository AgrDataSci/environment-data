# /// script
# requires-python = ">=3.12"
# dependencies = ["xarray", "zarr>=3", "dask", "fsspec", "aiohttp", "aiohttp-retry", "h5netcdf", "h5py", "pandas", "pyarrow"]
# ///
"""Daily CHIRTS-ERA5 Tmax and Tmin for points. One row per point and day, degrees C.

    uv run fetch.py points.csv out.parquet --start 2020-01-01 [--end 2025-12-31]
    uv run fetch.py points.csv out.parquet --date-col planting_date --days 100

points.csv needs columns id, lat, lon. Writes CSV if out ends in .csv, else parquet. end defaults to the latest day in the store.
With --date-col each point gets its own window, from its date (day 0) to --days later.
If Hugging Face is unreachable, it reads the same data from the CHC server's yearly NetCDF files instead."""

import argparse
import sys

import aiohttp
from aiohttp_retry import ExponentialRetry, RetryClient
import dask
import fsspec
import numpy as np
import pandas as pd
import xarray as xr

STORE = "https://huggingface.co/datasets/aaguilar90/chirts-era5-daily/resolve/main/chirts-era5-daily.zarr"
HOSTS = ["huggingface.co"]  # what a firewall must allow, plus the download host it redirects to (scripts/check.py names it)
FALLBACK = ["data.chc.ucsb.edu"]  # or this, the producer's own server
CHC = "https://data.chc.ucsb.edu/experimental/CHIRTS-ERA5/{v}/netcdf/daily/"
NETWORK = (OSError, aiohttp.ClientError)
THREADS = 32  # reads wait on the network, not the CPU
TMAX_OK = 60  # a few source pixels hold impossible values; mask above this


def open_store() -> xr.Dataset:
    # trust_env makes the HTTP client honour HTTPS_PROXY, which sandboxes and institutional networks route through
    return xr.open_zarr(STORE, consolidated=True, chunks={}, storage_options={"client_kwargs": {"trust_env": True}})


async def chc_client(**_):
    # data.chc.ucsb.edu answers 429 above a few requests a second; back off and retry
    retry = ExponentialRetry(attempts=10, start_timeout=0.5, statuses={429, 502, 503, 504})
    return RetryClient(client_session=aiohttp.ClientSession(trust_env=True), retry_options=retry)


def open_chc(points: pd.DataFrame, days: int | None) -> xr.Dataset:
    """The years the points need from the CHC server's yearly NetCDF files (chunked, so only the needed chunks are read).

    The same CHIRTS-ERA5 the Hugging Face store copies, returned in its names. Only years with both Tmax and Tmin are kept."""
    fs = fsspec.filesystem("https", get_client=chc_client)
    day0 = pd.to_datetime(points.date)
    last = (day0.max() + pd.Timedelta(days=days)).year if days is not None else pd.Timestamp.now().year
    parts = []
    for v, name in (("tmax", "Tmax"), ("tmin", "Tmin")):
        listed = set(fs.ls(CHC.format(v=v), detail=False))  # years not yet published are left out; select_daily reports the gap
        urls = [u for y in range(day0.min().year, last + 1) if (u := f"{CHC.format(v=v)}CHIRTS-ERA5.daily_{name}.{y}.nc") in listed]
        if not urls:
            raise ValueError(f"no CHIRTS-ERA5 {name} files on {FALLBACK[0]} for {day0.min().year} to {last}")
        parts.append(xr.concat([xr.open_dataset(fs.open(u, block_size=2**20), engine="h5netcdf", chunks={}).temp for u in urls], "time").rename(name))
    return xr.merge(parts, join="inner").rename(latitude="lat", longitude="lon")


def validate_points(points: pd.DataFrame) -> pd.DataFrame:
    """Unique ids so outputs join cleanly; WGS84 ranges catch swapped or projected coordinates."""
    if missing := {"id", "lat", "lon"} - set(points.columns):
        raise ValueError(f"points need columns id, lat, lon; missing {sorted(missing)}")
    points = points.reset_index(drop=True)
    points = points.assign(lat=pd.to_numeric(points.lat), lon=pd.to_numeric(points.lon))
    if points.id.isna().any() or points.id.duplicated().any():
        raise ValueError("points need unique, non-empty ids")
    for col, limit in (("lat", 90), ("lon", 180)):
        if len(bad := points.id[points[col].abs() > limit]):
            raise ValueError(f"{col} outside ±{limit} for ids {bad.tolist()}")
    return points


def periods(points: pd.DataFrame, start, end, days) -> tuple[pd.DataFrame, int | None]:
    """Each point's day 0 in `date`, and the days after it: one period for all (start, end), or each point's own date and days."""
    if days is None:
        if start is None:
            raise ValueError("give start, or days with a date column in points")
        days = (pd.Timestamp(end) - pd.Timestamp(start)).days if end else None  # None: to the store's last day
        return points.assign(date=start), days
    if "date" not in points:
        raise ValueError("a window per point needs a date column")
    points = points[pd.to_datetime(points.date).notna()].reset_index(drop=True)  # no date, no window
    if points.empty:
        raise ValueError("no point has a date")
    return points, days


def select_daily(ds, points, days, lat, lon):
    """Nearest cell per point, from its `date` (day 0) to `days` later, or to the store's last day if days is None.

    One vectorised read, so dask fetches each chunk once and in parallel. Off-grid points stay NaN; nearest alone would snap them to the edge cell."""
    times = ds.indexes["time"].normalize()
    day0 = pd.to_datetime(points.date).dt.normalize().values
    days = (times[-1] - day0.min()).days if days is None else days
    want = day0[:, None] + np.arange(max(days, 0) + 1) * np.timedelta64(1, "D")
    ti = times.get_indexer(want.ravel()).reshape(want.shape)
    if days < 0 or (ti < 0).any():  # else the store silently returns a shorter series
        raise ValueError(f"dates must be ordered and within {times[0].date()} to {times[-1].date()}; "
                         f"not so for ids {points.id[(ti < 0).any(axis=1)].tolist()}")
    idx = {}
    for col, name in (("lat", lat), ("lon", lon)):  # -1 when off grid or missing; it reads the last cell, then is masked
        half = abs(float(ds[name][1] - ds[name][0])) / 2 + 1e-6  # slack: points on a cell edge must not round off grid
        near = ds.indexes[name].get_indexer(points[col], method="nearest", tolerance=half)
        idx[name] = xr.DataArray(near, dims="pt", coords={"pt": points.id.values})
    one = (day0 == day0[0]).all()  # one period for all: a plain slice is far cheaper than a per-point time index
    idx["time"] = slice(ti[0, 0], ti[0, -1] + 1) if one else xr.DataArray(ti, dims=("pt", "day"))
    with dask.config.set(num_workers=THREADS):
        sub = ds.isel(idx).compute()
    if one:
        sub = sub.assign_coords(day=("time", np.arange(days + 1))).swap_dims(time="day").transpose("pt", "day")
    keep = (idx[lat] >= 0) & (idx[lon] >= 0)
    return sub.where(keep).assign_coords({n: sub[n].where(keep) for n in (lat, lon)})


def cli_points(p: argparse.ArgumentParser, a: argparse.Namespace) -> pd.DataFrame:
    """One period for all points (--start), or a window per point (--date-col with --days)."""
    if bool(a.start) == bool(a.date_col) or (a.date_col and (a.days is None or a.end)):
        p.error("give --start [--end] for one period, or --date-col with --days for a window per point")
    points = pd.read_csv(a.points, dtype={"id": str})
    if a.date_col and a.date_col != "date":
        if a.date_col not in points:
            p.error(f"no column {a.date_col} in {a.points}")
        points = points.drop(columns="date", errors="ignore").rename(columns={a.date_col: "date"})
    return points


def daily(ds: xr.Dataset, points: pd.DataFrame, days: int | None) -> pd.DataFrame:
    sub = select_daily(ds[["Tmax", "Tmin"]], points, days, "lat", "lon")
    sub = sub.where(sub <= TMAX_OK)
    df = sub.to_dataframe().reset_index().rename(columns={"pt": "id", "Tmax": "tmax_c", "Tmin": "tmin_c", "lat": "cell_lat", "lon": "cell_lon"})
    df["date"] = df.time.dt.date
    return df[["id", "date", "day", "tmax_c", "tmin_c", "cell_lat", "cell_lon"]].astype({"tmax_c": "float32", "tmin_c": "float32"})


def fetch(points: pd.DataFrame, start: str | None = None, end: str | None = None, days: int | None = None) -> pd.DataFrame:
    """One period for all points (start, end), or with days a window per point from its `date` column."""
    keep_day = days is not None
    points, days = periods(validate_points(points), start, end, days)
    try:
        out = daily(open_store(), points, days)
    except NETWORK as exc:  # a block, or a 404 if the store moved; check.py tells which
        print(f"could not read {HOSTS[0]} ({exc}); reading {FALLBACK[0]} instead, which is slower", file=sys.stderr)
        try:
            out = daily(open_chc(points, days), points, days)
        except NETWORK as exc:
            raise ConnectionError(f"could not read {HOSTS[0]} or {FALLBACK[0]} ({exc}). If the network blocks them, run scripts/check.py; see 'Network' in the repository SKILL.md") from None
    return out if keep_day else out.drop(columns="day")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("points"), p.add_argument("out"), p.add_argument("--start"), p.add_argument("--end")
    p.add_argument("--date-col", help="column with one date per point, such as the planting date")
    p.add_argument("--days", type=int, help="days to fetch after each point's date; 100 gives days 0 to 100")
    a = p.parse_args()
    points = cli_points(p, a)
    out = fetch(points, a.start, a.end, a.days)
    out.to_csv(a.out, index=False) if a.out.endswith(".csv") else out.to_parquet(a.out, index=False, compression="zstd")
    sea = out[["tmax_c", "tmin_c"]].isna().all(axis=1).groupby(out.id).all()
    print(f"{out.id.nunique()} points, {out.date.min()} to {out.date.max()}, {len(out):,} rows -> {a.out}")
    if sea.any():
        print(f"all-NaN (missing coordinates, outside coverage or source nodata): {list(sea[sea].index)}")
    if nodate := sorted(set(points.id) - set(out.id)):
        print(f"no date, so no window fetched: {nodate}")
