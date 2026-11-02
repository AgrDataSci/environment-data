---
name: agera5-climate
description: Get daily solar radiation, humidity, wind, reference evapotranspiration and vapour pressure deficit for points or farms anywhere on land, 1979 to about a week ago, from AgERA5 at 0.1° (~10 km). Needs a free Copernicus CDS key. Use when a user needs weather variables beyond rainfall and temperature, for disease pressure, water balance or crop models. For rainfall use chirps-rainfall and for temperature chirts-temperature, both finer and key-free.
---

# AgERA5 daily weather for points

AgERA5 is ECMWF's ERA5 reanalysis corrected for agriculture: daily, 0.1° (~10 km), 1979 to about a week ago, with agronomic variables such as FAO56 reference evapotranspiration already computed. ECMWF serves it as one analysis-ready Zarr cube, so you read only the cells and dates you need. It needs a free Copernicus key.

## Run

Put the points in a CSV with columns `id`, `lat`, `lon`, then

```sh
uv run scripts/fetch.py points.csv weather.parquet --start 2020-01-01 --end 2025-12-31
```

`--end` defaults to the latest day in the store. Needs only `uv` ([install](https://docs.astral.sh/uv/getting-started/installation/)) and a key (below). Output is one row per point and day: `id`, `date`, the variables, `cell_lat`, `cell_lon`. The script prints any point whose series is all NaN (sea or lake).

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

Use `tmax`, `tmin` and `precip` only outside CHIRPS and CHIRTS coverage (above 60°N or below 60°S) or when one source for everything matters more than resolution.

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

- A reanalysis, not observations. Rainfall in particular is poor in the tropics, which is why `chirps-rainfall` is the default for it.
- 0.1° cells smooth mountain terrain. Humidity and radiation in the East African highlands vary within a cell.
- Days are local time where CHIRPS and CHIRTS use UTC. Join on `id` and `date` and accept the offset.

## Combining with other sources

Pass the same points file and dates to `chirps-rainfall` and `chirts-temperature` and join on `id` and `date`, not cell coordinates (different grids). For hazards or stage summaries, pass the daily parquet to `climate-hazards`. When a calling skill asked for daily inputs, return them without starting hazard analysis.

## Cite

Boogaard H et al. (2020). Agrometeorological indicators from 1979 to present derived from reanalysis. Copernicus Climate Change Service (C3S) Climate Data Store. https://doi.org/10.24381/cds.6c68c9bb
