---
name: chirps-rainfall
description: Fetch daily CHIRPS v3 rainfall for farms, trial sites or other land points between 60°S and 60°N from 1981 onward. Use for precipitation time series; no API key needed.
---

# CHIRPS v3 daily rainfall for points

CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data) blends satellite rainfall estimates with rain-gauge records. It is the usual rainfall source for agriculture in Africa and the tropics. This reads CHIRPS v3.0 final, daily, 0.05° (~5 km), from the Icechunk store dynamical.org publishes on public S3. Open access, no key, nothing to download first.

## Run

Prepare a CSV with a unique, non-empty `id` per point and WGS84 decimal-degree `lat`, `lon` (longitude −180 to 180). Preserve identifiers, including leading zeros. Convert known missing-value sentinels to blank coordinates; do not guess corrections or move sites to obtain data. The script retains missing coordinates as NaN and rejects invalid ranges.

Resolve `scripts/fetch.py` relative to this skill's directory, not the user's working directory. Use absolute paths for the script, input and output when working elsewhere. Example from the skill directory:

```sh
uv run scripts/fetch.py points.csv rain.parquet --start 2020-01-01 --end 2025-12-31
uv run scripts/fetch.py points.csv rain.parquet --date-col planting_date --days 100
```

Give one period for all points with `--start`, or a window per point with `--date-col` and `--days`: each point is fetched from the date in that column (day 0) to `--days` later, and the output gains a `day` column counting from 0. Use the per-point form when points have their own dates, such as planting dates; it reads only the dates each point needs. A point with a blank date gets no rows and is listed. Use the requested period, asking if it is unspecified. `--end` defaults to the latest day actually in the store. Explicit dates outside store coverage fail with the available range; do not silently shorten a requested period. Needs only `uv` ([install](https://docs.astral.sh/uv/getting-started/installation/)); it fetches Python and the packages on first run. Output is one row per point and day: `id`, `date`, `precip_mm`, `cell_lat`, `cell_lon`. The script prints any point whose series is all NaN (missing coordinates, outside coverage or source nodata). Report these IDs and leave their coordinates unchanged.

The script needs `pypi.org` and `files.pythonhosted.org` on first run and `dynamical-ucsb-chc-chirps.s3.us-west-2.amazonaws.com` for the data. If it reports a host it cannot reach, the network is blocked, not the data missing: say so, name the host, and give the user the command to run on an open network. Do not switch source or estimate values.

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

For Tmax and Tmin, pass the same points file and dates to `chirts-temperature`; both have 0.05° resolution; join on `id` and `date` and retain each source's selected cell coordinates. For radiation, humidity, wind or ET use `agera5-climate` (0.1° grid, local-time days); join on `id` and `date`, not cell coordinates. The `climate-hazards` skill is planned and is not bundled here. Return daily data; compute summaries only when requested, using an available workflow with explicit definitions.

## Deliver

Return the output path, actual date range, units, source/store, grid resolution and missing-data counts per variable. Include retrieval date and any filtering. A successful fetch can still contain daily gaps; check completeness before totals or threshold counts.

## Cite

Use the CHIRPS **v3** attribution in the [store catalogue](https://dynamical.org/catalog/ucsb-chc-chirps-analysis-final/): Funk C et al. (2026), The Climate Hazards Center Infrared Precipitation with Stations, Version 3, Scientific Data 13, 718. Data DOI: https://doi.org/10.15780/G2JQ0P. Credit the dynamical.org redistribution (CC BY 4.0). The 2015 CHIRPS paper describes the earlier product.
