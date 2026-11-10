# /// script
# requires-python = ">=3.12"
# dependencies = ["xarray", "zarr>=3", "dask", "fsspec", "aiohttp", "pandas", "pyarrow"]
# ///
"""Daily AgERA5 weather for points. One row per point and day.

    uv run fetch.py points.csv weather.parquet --start 2020-01-01 [--end 2025-12-31] [--vars srad,rhmax,rhmin,wind,et0,vpd]
    uv run fetch.py points.csv weather.parquet --date-col planting_date --days 100

points.csv needs columns id, lat, lon. Writes CSV if out ends in .csv, else parquet. Needs a Copernicus CDS key in CDSAPI_KEY or ~/.cdsapirc.
With --date-col each point gets its own window, from its date (day 0) to --days later.
Default variables are the ones CHIRPS and CHIRTS do not have; the rest are available by name."""

import argparse
import os
import urllib.error
import urllib.request
from pathlib import Path

import dask
import pandas as pd
import xarray as xr

STORE = "https://arco.datastores.ecmwf.int/cadl-arco-geo-001/arco/sis_agrometeorological_indicators/all/geoChunked.zarr"
HOSTS = ["arco.datastores.ecmwf.int"]  # what a firewall must allow
THREADS = 32  # reads wait on the network, not the CPU
TILE = 3  # degrees; close to the store's spatial chunk, so grouped reads rarely fetch a chunk twice
# friendly name -> (store variable, scale, offset) so that value = store * scale + offset
VARS = {
    "srad": ("Solar_Radiation_Flux", 1e-6, 0),  # J m-2 d-1 -> MJ m-2 d-1
    "rhmax": ("Derived_Relative_Humidity_2m_Max_24h", 1, 0),  # %
    "rhmin": ("Derived_Relative_Humidity_2m_Min_24h", 1, 0),
    "wind": ("Wind_Speed_10m_Mean_24h", 1, 0),  # m s-1 at 10 m
    "et0": ("ReferenceET_PenmanMonteith_FAO56", 1, 0),  # mm d-1
    "vpd": ("Vapour_Pressure_Deficit_at_Maximum_Temperature", 1, 0),  # hPa
    "tmax": ("Temperature_Air_2m_Max_24h", 1, -273.15),  # K -> C
    "tmin": ("Temperature_Air_2m_Min_24h", 1, -273.15),
    "tmean": ("Temperature_Air_2m_Mean_24h", 1, -273.15),
    "precip": ("Precipitation_Flux", 1, 0),  # mm d-1
}
DEFAULT = "srad,rhmax,rhmin,wind,et0,vpd"


def cds_key() -> str:
    """CDSAPI_KEY, else the key line of ~/.cdsapirc. Never printed."""
    if key := os.environ.get("CDSAPI_KEY"):
        return key
    rc = Path.home() / ".cdsapirc"
    if rc.exists():
        for line in rc.read_text().splitlines():
            if line.strip().startswith("key:"):
                return line.split(":", 1)[1].strip()
    raise SystemExit("No CDS key. Set CDSAPI_KEY or write ~/.cdsapirc; see SKILL.md for how to get one.")


def reachable(hosts: list[str]) -> None:
    """Fail early and name the host when a firewall or sandbox blocks the store. Any HTTP answer counts as reachable."""
    for host in hosts:
        try:
            urllib.request.urlopen(urllib.request.Request(f"https://{host}/", method="HEAD"), timeout=20)
        except urllib.error.HTTPError:
            pass  # the host answered
        except OSError as exc:
            raise ConnectionError(
                f"cannot reach {host} ({exc}). This is a network block, not missing data. Allow {', '.join(hosts)} "
                "or run the same command on a machine with open internet. See 'Network' in the repository SKILL.md."
            ) from None


def open_store() -> xr.Dataset:
    # trust_env makes the HTTP client honour HTTPS_PROXY, which sandboxes and institutional networks route through
    options = {"headers": {"Authorization": f"Bearer {cds_key()}"}, "client_kwargs": {"trust_env": True}}
    return xr.open_zarr(STORE, consolidated=True, chunks={}, storage_options=options)


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


def windows(points: pd.DataFrame, days: int):
    """Yield (points, start, end) reads that cover each point's own window, its date to `days` later.

    Points in one spatial chunk whose windows overlap share a read, so no chunk is fetched for dates nobody asked for."""
    if "date" not in points or days < 0:
        raise ValueError("a window per point needs a date column and days of 0 or more")
    pts = points.assign(date=pd.to_datetime(points.date).dt.normalize())
    pts = pts[pts.date.notna()].sort_values("date")  # points without a date have no window
    if pts.empty:
        raise ValueError("no point has a date")
    span = pd.Timedelta(days=days)
    for _, tile in pts.groupby([(pts[c] // TILE).fillna(-999) for c in ("lat", "lon")]):
        run = (tile.date > (tile.date + span).cummax().shift()).cumsum()  # a new read where windows stop overlapping
        for _, g in tile.groupby(run):
            yield g.reset_index(drop=True), g.date.min(), g.date.max() + span


def clip(parts: list[pd.DataFrame], points: pd.DataFrame, days: int) -> pd.DataFrame:
    """Keep each point's own window and count its days, 0 on the point's date."""
    df = pd.concat(parts, ignore_index=True)
    day0 = pd.to_datetime(points.set_index("id").date).dt.normalize()
    df.insert(2, "day", (pd.to_datetime(df.date) - df.id.map(day0)).dt.days)
    return df[df.day.between(0, days)].sort_values(["id", "date"]).reset_index(drop=True)


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


def daily(ds: xr.Dataset, points: pd.DataFrame, start, end, names: list[str]) -> pd.DataFrame:
    sub = select_daily(ds[[VARS[n][0] for n in names]], points, start, end, "latitude", "longitude")
    df = sub.to_dataframe().reset_index().rename(columns={"latitude": "cell_lat", "longitude": "cell_lon"})
    for n in names:
        var, scale, offset = VARS[n]
        df[n] = (df.pop(var) * scale + offset).astype("float32")
    df["id"] = points.id.values[df.pt]
    df["date"] = df.time.dt.date
    return df[["id", "date", *names, "cell_lat", "cell_lon"]]


def fetch(points: pd.DataFrame, start: str | None = None, end: str | None = None, names: list[str] | None = None, days: int | None = None) -> pd.DataFrame:
    """One period for all points (start, end), or with days a window per point from its `date` column."""
    points = validate_points(points)
    names = DEFAULT.split(",") if names is None else names
    if bad := set(names) - set(VARS):
        raise ValueError(f"unknown variables {sorted(bad)}; choose from {', '.join(VARS)}")
    if days is None and start is None:
        raise ValueError("give start, or days with a date column in points")
    reachable(HOSTS)
    ds = open_store()
    if days is None:
        return daily(ds, points, start, end, names)
    return clip([daily(ds, g, s, e, names) for g, s, e in windows(points, days)], points, days)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("points"), p.add_argument("out"), p.add_argument("--start"), p.add_argument("--end")
    p.add_argument("--date-col", help="column with one date per point, such as the planting date")
    p.add_argument("--days", type=int, help="days to fetch after each point's date; 100 gives days 0 to 100")
    p.add_argument("--vars", default=DEFAULT, help=f"comma-separated from {', '.join(VARS)}")
    a = p.parse_args()
    names = a.vars.split(",")
    points = cli_points(p, a)
    out = fetch(points, a.start, a.end, names, a.days)
    out.to_csv(a.out, index=False) if a.out.endswith(".csv") else out.to_parquet(a.out, index=False, compression="zstd")
    print(f"{out.id.nunique()} points, {out.date.min()} to {out.date.max()}, {len(out):,} rows, {names} -> {a.out}")
    miss = out[names].isna().all(axis=1).groupby(out.id).all()
    if miss.any():
        print(f"all-NaN: {list(miss[miss].index)}")
    if nodate := sorted(set(points.id) - set(out.id)):
        print(f"no date, so no window fetched: {nodate}")
