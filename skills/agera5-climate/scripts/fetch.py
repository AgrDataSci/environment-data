# /// script
# requires-python = ">=3.12"
# dependencies = ["xarray", "zarr>=3", "dask", "fsspec", "aiohttp", "pandas", "pyarrow"]
# ///
"""Daily AgERA5 weather for points. One row per point and day.

    uv run fetch.py points.csv weather.parquet --start 2020-01-01 [--end 2025-12-31] [--vars srad,rhmax,rhmin,wind,et0,vpd]

points.csv needs columns id, lat, lon. Needs a Copernicus CDS key in CDSAPI_KEY or ~/.cdsapirc.
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


def fetch(points: pd.DataFrame, start: str, end: str | None = None, names: list[str] = DEFAULT.split(",")) -> pd.DataFrame:
    ds = xr.open_zarr(STORE, consolidated=True, chunks={}, storage_options={"headers": {"Authorization": f"Bearer {cds_key()}"}})
    sel = dict(latitude=xr.DataArray(points.lat.values, dims="pt"), longitude=xr.DataArray(points.lon.values, dims="pt"))
    store_vars = [VARS[n][0] for n in names]
    with dask.config.set(num_workers=THREADS):
        sub = ds[store_vars].sel(time=slice(start, end)).sel(**sel, method="nearest").compute()
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
    if bad := set(names) - set(VARS):
        raise SystemExit(f"unknown variables {sorted(bad)}; choose from {', '.join(VARS)}")
    out = fetch(pd.read_csv(a.points), a.start, a.end, names)
    out.to_parquet(a.out, index=False, compression="zstd")
    print(f"{out.id.nunique()} points, {out.date.min()} to {out.date.max()}, {len(out):,} rows, {names} -> {a.out}")
    miss = out.groupby("id")[names[0]].apply(lambda s: s.isna().all())
    if miss.any():
        print(f"all-NaN: {list(miss[miss].index)}")
