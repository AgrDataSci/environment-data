# /// script
# requires-python = ">=3.12"
# dependencies = ["xarray", "zarr>=3", "icechunk", "dask", "pandas", "pyarrow"]
# ///
"""Daily CHIRPS v3 rainfall for points. One row per point and day, mm.

    uv run fetch.py points.csv out.parquet --start 2020-01-01 [--end 2025-12-31]
    uv run fetch.py points.csv out.parquet --date-col planting_date --days 100

points.csv needs columns id, lat, lon. Writes CSV if out ends in .csv, else parquet. end defaults to the latest day in the store.
With --date-col each point gets its own window, from its date (day 0) to --days later."""

import argparse

import dask
import numpy as np
import icechunk
import pandas as pd
import xarray as xr

STORE = ("dynamical-ucsb-chc-chirps", "ucsb-chc-chirps-analysis-final/v0.1.0.icechunk")
HOSTS = ["dynamical-ucsb-chc-chirps.s3.us-west-2.amazonaws.com"]  # what a firewall must allow
THREADS = 32  # reads wait on the network, not the CPU


def open_store() -> xr.Dataset:
    storage = icechunk.s3_storage(bucket=STORE[0], prefix=STORE[1], region="us-west-2", anonymous=True)
    session = icechunk.Repository.open(storage).readonly_session("main")
    return xr.open_zarr(session.store, consolidated=False, chunks={})


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
    sub = select_daily(ds.precipitation_surface, points, days, "latitude", "longitude")
    df = (sub * 86400).to_dataframe(name="precip_mm").reset_index()  # kg m-2 s-1 -> mm/day
    df["date"] = df.time.dt.date
    df = df.rename(columns={"pt": "id", "latitude": "cell_lat", "longitude": "cell_lon"})
    return df[["id", "date", "day", "precip_mm", "cell_lat", "cell_lon"]].astype({"precip_mm": "float32"})


def fetch(points: pd.DataFrame, start: str | None = None, end: str | None = None, days: int | None = None) -> pd.DataFrame:
    """One period for all points (start, end), or with days a window per point from its `date` column."""
    keep_day = days is not None
    points, days = periods(validate_points(points), start, end, days)
    try:
        out = daily(open_store(), points, days)
    except (OSError, icechunk.StorageError) as exc:  # a block, or a 404 if the store moved; check.py tells which
        raise ConnectionError(f"could not read {', '.join(HOSTS)} ({exc}). If the network blocks it, run scripts/check.py; see 'Network' in the repository SKILL.md") from None
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
    sea = out.groupby("id").precip_mm.apply(lambda s: s.isna().all())
    print(f"{out.id.nunique()} points, {out.date.min()} to {out.date.max()}, {len(out):,} rows -> {a.out}")
    if sea.any():
        print(f"all-NaN (missing coordinates, outside coverage or source nodata): {list(sea[sea].index)}")
    if nodate := sorted(set(points.id) - set(out.id)):
        print(f"no date, so no window fetched: {nodate}")
