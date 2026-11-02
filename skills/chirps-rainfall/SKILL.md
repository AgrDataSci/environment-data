---
name: chirps-rainfall
description: Get daily rainfall time series for points or farms on land between 60°S and 60°N, 1981 to about a month ago, from CHIRPS v3 at 0.05° (~5 km). No API key needed. Use when a user wants rainfall or precipitation data for trial sites, farms, coordinates or a dataset of locations, especially in the tropics. For temperature use chirts-temperature; for other weather variables, use agera5-climate.
---

# CHIRPS v3 daily rainfall for points

CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data) blends satellite rainfall estimates with rain-gauge records. It is the usual rainfall source for agriculture in Africa and the tropics. This reads CHIRPS v3.0 final, daily, 0.05° (~5 km), from the Icechunk store dynamical.org publishes on public S3. Open access, no key, nothing to download first.

## Run

Put the points in a CSV with columns `id`, `lat`, `lon`, then

```sh
uv run scripts/fetch.py points.csv rain.parquet --start 2020-01-01 --end 2025-12-31
```

`--end` defaults to the latest day in the store. Needs only `uv` ([install](https://docs.astral.sh/uv/getting-started/installation/)); it fetches Python and the packages on first run. Output is one row per point and day: `id`, `date`, `precip_mm`, `cell_lat`, `cell_lon`. The script prints any point whose series is all NaN (sea or off-grid); move it inland and rerun.

## If you need to read the store yourself

```python
import icechunk, xarray as xr
storage = icechunk.s3_storage(bucket="dynamical-ucsb-chc-chirps",
    prefix="ucsb-chc-chirps-analysis-final/v0.1.0.icechunk", region="us-west-2", anonymous=True)
ds = xr.open_zarr(icechunk.Repository.open(storage).readonly_session("main").store, consolidated=False, chunks={})
```

- One variable, `precipitation_surface`, in **kg m⁻² s⁻¹**. Multiply by 86400 for mm per day. Dims `(time, latitude, longitude)`.
- Latitude is descending. Slice north to south: `latitude=slice(north, south)`.
- Land only, 60°S to 60°N. Sea cells are NaN, so check coastal points after fetching.
- Chunks are 365 days × 3° × 3°, so a point read costs one small request per year per 3° tile. Select dates first, pick all points in one vectorised `sel(method="nearest")`, `.compute()` once, and raise dask's thread count (reads are network-bound). Cost scales with the number of 3° tiles the points cover, not the number of points.

For a regional map rather than points, the Hugging Face copy `aaguilar90/chirps-v3-daily-rnl` is cheaper (15° chunks); from R it reads with `terra` and GDAL 3.11+, values × 0.1.

## Combining with other sources

For Tmax and Tmin, pass the same points file and dates to `chirts-temperature`; both share the 0.05° grid, so join on `id` and `date`. For radiation, humidity, wind or ET use `agera5-climate` (0.1° grid, local-time days); join on `id` and `date`, not cell coordinates. For season totals, dry spells or stage summaries, pass the daily parquet to `climate-hazards`. When a calling skill asked for daily inputs, return them without starting hazard analysis.

## Cite

Funk C et al. (2015). The climate hazards infrared precipitation with stations. Scientific Data 2, 150066. https://doi.org/10.1038/sdata.2015.66. Store: dynamical.org, https://dynamical.org/catalog/ucsb-chc-chirps-analysis-final/
