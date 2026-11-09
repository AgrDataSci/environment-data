# /// script
# requires-python = ">=3.12"
# dependencies = ["xarray", "zarr>=3", "dask", "fsspec", "aiohttp", "pandas", "pyarrow"]
# ///
"""Daily AgERA5 weather for points. One row per point and day.

    uv run fetch.py points.csv weather.parquet --start 2020-01-01 [--end 2025-12-31] [--vars srad,rhmax,rhmin,wind,et0,vpd]

points.csv needs columns id, lat, lon. Writes CSV if out ends in .csv, else parquet. Needs a Copernicus CDS key in CDSAPI_KEY or ~/.cdsapirc.
Default variables are the ones CHIRPS and CHIRTS do not have; the rest are available by name."""

import argparse
import os
from pathlib import Path

import dask
import pandas as pd
import xarray as xr

STORE = "https://arco.datastores.ecmwf.int/cadl-arco-geo-001/arco/sis_agrometeorological_indicators/all/geoChunked.zarr"
THREADS = 32  # reads wait on the network, not the CPU
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


def fetch(points: pd.DataFrame, start: str, end: str | None = None, names: list[str] | None = None) -> pd.DataFrame:
    points = validate_points(points)
    names = DEFAULT.split(",") if names is None else names
    if bad := set(names) - set(VARS):
        raise ValueError(f"unknown variables {sorted(bad)}; choose from {', '.join(VARS)}")
    ds = xr.open_zarr(STORE, consolidated=True, chunks={}, storage_options={"headers": {"Authorization": f"Bearer {cds_key()}"}})
    store_vars = [VARS[n][0] for n in names]
    sub = select_daily(ds[store_vars], points, start, end, "latitude", "longitude")
    df = sub.to_dataframe().reset_index().rename(columns={"latitude": "cell_lat", "longitude": "cell_lon"})
    for n in names:
        var, scale, offset = VARS[n]
        df[n] = (df.pop(var) * scale + offset).astype("float32")
    df["id"] = points.id.values[df.pt]
    df["date"] = df.time.dt.date
    return df[["id", "date", *names, "cell_lat", "cell_lon"]]


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("points"), p.add_argument("out"), p.add_argument("--start", required=True), p.add_argument("--end")
    p.add_argument("--vars", default=DEFAULT, help=f"comma-separated from {', '.join(VARS)}")
    a = p.parse_args()
    names = a.vars.split(",")
    out = fetch(pd.read_csv(a.points, dtype={"id": str}), a.start, a.end, names)
    out.to_csv(a.out, index=False) if a.out.endswith(".csv") else out.to_parquet(a.out, index=False, compression="zstd")
    print(f"{out.id.nunique()} points, {out.date.min()} to {out.date.max()}, {len(out):,} rows, {names} -> {a.out}")
    miss = out[names].isna().all(axis=1).groupby(out.id).all()
    if miss.any():
        print(f"all-NaN: {list(miss[miss].index)}")
