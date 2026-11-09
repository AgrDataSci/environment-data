---
name: agera5-climate
description: Fetch daily AgERA5 radiation, humidity, wind, reference evapotranspiration and other weather variables for land points from 1979 onward. Use for crop-model or water-balance inputs; requires a Copernicus CDS key.
---

# AgERA5 daily weather for points

AgERA5 is ECMWF's ERA5 reanalysis corrected for agriculture: daily, 0.1° (~10 km), 1979 onward (check the store for the latest date), with agronomic variables such as FAO56 reference evapotranspiration already computed. ECMWF serves it as one analysis-ready Zarr cube, so you read only the cells and dates you need. It needs a free Copernicus key.

## Run

Prepare a CSV with a unique, non-empty `id` per point and WGS84 decimal-degree `lat`, `lon` (longitude −180 to 180). Preserve identifiers, including leading zeros. Convert known missing-value sentinels to blank coordinates; do not guess corrections or move sites to obtain data. The script retains missing coordinates as NaN and rejects invalid ranges.

Resolve `scripts/fetch.py` relative to this skill's directory, not the user's working directory. Use absolute paths for the script, input and output when working elsewhere. Example from the skill directory:

```sh
uv run scripts/fetch.py points.csv weather.parquet --start 2020-01-01 --end 2025-12-31
```

`--start` is required; use the requested period, asking if it is unspecified. `--end` defaults to the latest day actually in the store. Explicit dates outside store coverage fail with the available range; do not silently shorten a requested period. Needs only `uv` ([install](https://docs.astral.sh/uv/getting-started/installation/)) and a key (below). Output is one row per point and day: `id`, `date`, the variables, `cell_lat`, `cell_lon`. The script prints any point whose series is all NaN (missing coordinates, outside coverage or source nodata). Report these IDs without moving the points.

Default variables are the ones CHIRPS and CHIRTS don't have. Pick others with `--vars`:

| Name | Variable | Unit |
|---|---|---|
| `srad` | solar radiation | MJ m⁻² d⁻¹ |
| `rhmax`, `rhmin` | relative humidity, daily max and min | % |
| `wind` | wind speed at 10 m, daily mean | m s⁻¹ |
| `et0` | FAO56 Penman-Monteith reference ET | mm d⁻¹ |
| `vpd` | vapour pressure deficit at the daily maximum temperature | hPa |
| `tmax`, `tmin`, `tmean` | air temperature at 2 m | °C |
| `precip` | rainfall | mm d⁻¹ |

Use `tmax`, `tmin` and `precip` when the user requests AgERA5, needs consistent variables from one source, or is outside the other sources' coverage: CHIRPS is 60°S–60°N; CHIRTS is 60°S–70°N.

## Key

The script looks for `CDSAPI_KEY` in the environment, then the `key:` line of `~/.cdsapirc`. It never prints the key. If neither exists, tell the user how to get one (free, a few minutes) and stop:

1. Register or log in at https://cds.climate.copernicus.eu.
2. On the profile page, copy the Personal Access Token.
3. Accept the dataset licence: open "Agrometeorological indicators from 1979 to present derived from reanalysis", go to the Download tab, and accept the terms. A valid key is refused until this is done.
4. Save it, either `export CDSAPI_KEY=<token>` or in `~/.cdsapirc`:
   ```
   url: https://cds.climate.copernicus.eu/api
   key: <token>
   ```

## If you need to read the store yourself

```python
import xarray as xr
ds = xr.open_zarr("https://arco.datastores.ecmwf.int/cadl-arco-geo-001/arco/sis_agrometeorological_indicators/all/geoChunked.zarr",
                  consolidated=True, chunks={}, storage_options={"headers": {"Authorization": f"Bearer {key}"}})
```

- 27 variables, `ds.data_vars` lists them. Temperatures in K, radiation in J m⁻² d⁻¹, missing is `-9999` and decodes to NaN.
- Latitude ascending from -90, longitude from -180. Chunks are 2112 days × 16 × 32 cells, so long periods at a few places are cheap and one day over a region is slow.
- Each day runs midnight to midnight local time, not UTC.
- Select variables and dates first, pick all points in one vectorised `sel(method="nearest")`, `.compute()` once, raise dask's thread count.

## Caveats

- A reanalysis, not station observations. Evaluate suitability for the region and purpose; higher spatial resolution alone does not establish accuracy.
- 0.1° cells smooth mountain terrain. Humidity and radiation in the East African highlands vary within a cell.
- AgERA5 days use local time ([CDS documentation](https://cds.climate.copernicus.eu/datasets/sis-agrometeorological-indicators?tab=overview)). Check the other source's day definition before joining on `id` and `date`; matching date labels do not guarantee matching 24-hour windows. Disclose any mismatch, especially for event comparisons.

## Combining with other sources

Pass the same points file and dates to `chirps-rainfall` and `chirts-temperature` and join on `id` and `date`, not cell coordinates (different grids). The `climate-hazards` skill is planned and is not bundled here. Return daily data; compute summaries only when requested, using an available workflow with explicit definitions.

## Deliver

Return the output path, actual date range, units, source/store, grid resolution and missing-data counts per variable. Include retrieval date and any filtering. A successful fetch can still contain daily gaps; check completeness before totals or threshold counts.

## Cite

Boogaard H et al. (2020). Agrometeorological indicators from 1979 to present derived from reanalysis. Copernicus Climate Change Service (C3S) Climate Data Store. https://doi.org/10.24381/cds.6c68c9bb
