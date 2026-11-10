---
name: chirts-temperature
description: Fetch daily CHIRTS-ERA5 maximum and minimum temperature for land points between 60°S and 70°N from 1981 onward. Use for temperature time series or heat-analysis inputs; no API key needed.
---

# CHIRTS-ERA5 daily temperature for points

CHIRTS (Climate Hazards Center InfraRed Temperature with Stations) estimates 2 m air temperature from infrared satellite data blended with station records. This reads **CHIRTS-ERA5**, the ERA5-driven reconstruction that extends the record to the present, from the Zarr store on Hugging Face (`aaguilar90/chirts-era5-daily`, CC BY 4.0). Same 0.05° grid as CHIRPS. Open access, no key, nothing to download first.

## Run

Prepare a CSV with a unique, non-empty `id` per point and WGS84 decimal-degree `lat`, `lon` (longitude −180 to 180). Preserve identifiers, including leading zeros. Convert known missing-value sentinels to blank coordinates; do not guess corrections or move sites to obtain data. The script retains missing coordinates as NaN and rejects invalid ranges.

Resolve `scripts/fetch.py` relative to this skill's directory, not the user's working directory. Use absolute paths for the script, input and output when working elsewhere. Example from the skill directory:

```sh
uv run scripts/fetch.py points.csv temp.parquet --start 2020-01-01 --end 2025-12-31
uv run scripts/fetch.py points.csv temp.parquet --date-col planting_date --days 100
```

Give one period for all points with `--start`, or a window per point with `--date-col` and `--days`: each point is fetched from the date in that column (day 0) to `--days` later, and the output gains a `day` column counting from 0. Use the per-point form when points have their own dates, such as planting dates; it reads only the dates each point needs. A point with a blank date gets no rows and is listed. Use the requested period, asking if it is unspecified. `--end` defaults to the latest day actually in the store. Explicit dates outside store coverage fail with the available range; do not silently shorten a requested period. Needs only `uv` ([install](https://docs.astral.sh/uv/getting-started/installation/)); it fetches Python and the packages on first run. Output is one row per point and day: `id`, `date`, `tmax_c`, `tmin_c`, `cell_lat`, `cell_lon`. Values above 60 °C are masked (a few source pixels hold impossible values). The script prints any point whose series is all NaN (missing coordinates, outside coverage or source nodata). Report these IDs and leave their coordinates unchanged.

The script needs `pypi.org` and `files.pythonhosted.org` on first run and `huggingface.co` and the download host it redirects to (`scripts/check.py` in the repository names it) for the data. If it reports a host it cannot reach, the network is blocked, not the data missing: say so, name the host, and give the user the command to run on an open network. Do not switch source or estimate values.

## Bandwidth

Chunks are 128 days × 15° × 15°, one file each, so a point read fetches the whole 15° tile for that period. Measured for ten points in Rwanda, both variables: one year moved 110 MB, six years 500 MB. Points in the same tile share the cost, so a cluster of farms costs the same as one farm. On a slow link, fetch only the dates you need and expect minutes per year. These are measurements from this store, not a guarantee for every region or connection.

## If you need to read the store yourself

```python
import xarray as xr
ds = xr.open_zarr("https://huggingface.co/datasets/aaguilar90/chirts-era5-daily/resolve/main/chirts-era5-daily.zarr",
                  consolidated=True, chunks={})
```

- Two variables, `Tmax` and `Tmin`, in °C, stored as int16 in 0.1 °C steps; xarray decodes them and turns `-32768` into NaN. Dims `(time, lat, lon)`.
- Latitude is descending. Slice north to south: `lat=slice(north, south)`.
- Land only, 60°S to 70°N. Sea and lake cells are NaN every day.
- Select dates first, pick all points in one vectorised `sel(method="nearest")`, `.compute()` once, raise dask's thread count.
- From R, `terra::rast('ZARR:"path.zarr":/Tmax')` works with GDAL 3.11 or newer, but returns raw integers: multiply by 0.1 and set `NAflag` to -32768.

## Caveats

- CHIRTS-ERA5 is in the Climate Hazards Center's experimental tree, not the validated, station-blended CHIRTS-daily v1.0 (1983 to 2016 only). Don't mix the two. Do not transfer validation claims from CHIRTS-daily to this reconstruction.
- Values are satellite estimates, not station readings. Local extremes can differ from any single station.

## Combining with other sources

For rainfall, pass the same points file and dates to `chirps-rainfall`; join on `id` and `date` and retain each source's selected cell coordinates. For humidity, radiation, wind or ET use `agera5-climate` (0.1° grid, local-time days; its script converts temperatures to °C); keep its units and join on `id` and `date`. The `climate-hazards` skill is planned and is not bundled here. Return daily data; compute summaries only when requested, using an available workflow with explicit definitions.

## Deliver

Return the output path, actual date range, units, source/store, grid resolution and missing-data counts per variable. Include retrieval date and any filtering (such as temperature screening). A successful fetch can still contain daily gaps; check completeness before totals or threshold counts.

## Cite

Identify the actual product as CHIRTS-ERA5 and cite the [redistributed dataset](https://huggingface.co/datasets/aaguilar90/chirts-era5-daily) with its access date. Background method paper: Funk C et al. (2019). A high-resolution 1983–2016 Tmax climate data record based on infrared temperatures and stations. Journal of Climate 32, 5639–5658. https://doi.org/10.1175/JCLI-D-18-0698.1. Store: https://huggingface.co/datasets/aaguilar90/chirts-era5-daily
