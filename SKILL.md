---
name: environment-data
description: Select environment-data skills for daily weather, surface elevation or soil properties at farms, trial sites or other coordinates. Use to choose sources or combine point-based outputs.
---

# Environment data for points

Daily weather, surface elevation and soil properties for sets of points, as one skill per source. Every skill takes points as `id`, `lat`, `lon` and returns a table with one row per point and day (one row per point for elevation and soil), so weather outputs join on `id` and `date`, and static outputs join on `id`. Boundaries, hazards and future climate are planned; do not try to invoke them as bundled skills.

## Names

Write maniva and tricot in lowercase, always. That holds in headings, table cells, lists and at the start of a sentence, so never write "Maniva" or "Tricot". tricot stands for triadic comparison of technology options. Quote published titles exactly as printed, even where they capitalise tricot or expand it as "triadic comparisons of technologies".

## Which skill

| Need | Skill | Source | Status |
|---|---|---|---|
| Daily rainfall | [chirps-rainfall](skills/chirps-rainfall/SKILL.md) | CHIRPS v3, 0.05°, 1981 onward; check store end date | ready |
| Daily Tmax and Tmin | [chirts-temperature](skills/chirts-temperature/SKILL.md) | CHIRTS-ERA5, 0.05°, 1981 onward; check store end date | ready |
| Radiation, humidity, wind, ET | [agera5-climate](skills/agera5-climate/SKILL.md) | AgERA5, 0.1°, needs a free CDS key | ready |
| Elevation | [elevation](skills/elevation/SKILL.md) | Copernicus 90 m DEM | ready |
| Soil texture, carbon, nitrogen, pH, CEC, bulk density | [soilgrids-soil](skills/soilgrids-soil/SKILL.md) | SoilGrids 2.0, 250 m or 1 km mean, six depths | ready |
| District and province | `admin-boundaries` | GAUL 2025 | planned |
| Dry spells, hot days, heavy rain, GDD per season or stage | `climate-hazards` | any daily series | planned |
| How those hazards shift to 2099 | `future-climate` | World Bank CCKP, CMIP6 | planned |

## Three ways to use a skill

1. **Run the script.** Resolve `scripts/fetch.py` from the selected skill directory and follow its CLI example (weather also requires a period, see "Dates"). Use this by default. Output is parquet; name the output `.csv` for CSV, which suits elevation, soil and small weather pulls (daily weather for many points gets large).
2. **Import the script module.** Its `fetch()` takes a pandas data frame with `id`, `lat`, `lon` (and `date` for a window per point) and returns a data frame. Use it inside a larger Python pipeline.
3. **Write your own reader.** Each skill's "If you need to read ... yourself" section gives the store path and the traps (units, scaling, projection, grid order). Use it for R, other tools or needs the script does not cover.

## Points

Users give points in any form: a table with other column names, Excel, tricot data, or coordinates typed in chat. Build the `id`, `lat`, `lon` table yourself, using an existing identifier as `id` or the row number if there is none. Tell the user which columns you used. Use WGS84 decimal degrees with `lat` within −90 to 90 and `lon` within −180 to 180. Preserve IDs as strings and require unique, non-empty IDs for unambiguous joins. Investigate suspected swapped coordinates; never guess a correction or shift points to obtain values. Convert known missing-coordinate sentinels to blanks. Leave rows with missing coordinates in; the scripts return them as NaN.

For a tricot-data trial file, take `site_data[]`: `site_id` → `id`, `latitude` → `lat`, `longitude` → `lon`, and keep `planting_date` for a window per site (`--date-col planting_date`). Some sites have null coordinates or planting dates; keep them as blanks. Other trial tables follow the same pattern: one row per plot or block, its identifier as `id`, its coordinates and its planting date. tricot site coordinates are displaced up to 1.5 km for privacy (`metadata.location_privacy`), and some are filled from the mean of their district (`coordinate_source`, `coordinate_uncertainty_m`). That is fine for 5 to 10 km weather grids. For elevation (90 m) and soil (250 m), values describe the published point, not the farm; say so when you report them.

## Dates

The weather skills take a period in one of two ways. Use the same one for every source you combine.

- **One period for all points.** `--start 2020-01-01 --end 2020-12-31`. Use it for a season, a year or a climatology.
- **A window per point.** `--date-col planting_date --days 100` fetches each point from its own date (day 0) to 100 days later, 101 rows per point, with a `day` column counting from 0. Use it whenever points have their own dates, such as planting dates per plot or survey dates per household. Points planted weeks or years apart then cost only their own windows, not the whole span between the first and the last.

Keep the date column in the points file under its own name and pass that name to `--date-col`. Dates are ISO (`YYYY-MM-DD`). A point with a blank date gets no rows and the script lists it; report those IDs. A window that runs past the store's last day fails rather than being shortened. Ask the user for the number of days if they did not give one; the crop's season length is theirs to state.

## Network

The scripts read remote stores, and `uv` installs their packages on first run, so they need these hosts. Sandboxed agents and institutional networks often block them.

| Need | Hosts |
|---|---|
| Packages, first run of any script | `pypi.org`, `files.pythonhosted.org` |
| `chirps-rainfall` | `dynamical-ucsb-chc-chirps.s3.us-west-2.amazonaws.com` |
| `chirts-temperature` | `huggingface.co` and the download host it redirects to, which `scripts/check.py` names |
| `agera5-climate` | `arco.datastores.ecmwf.int` |
| `elevation` | `copernicus-dem-90m.s3.amazonaws.com` |
| `soilgrids-soil` | `files.isric.org` |

`python3 scripts/check.py [skill ...]` tests them with the standard library only, so it runs where nothing can be installed. Each script also checks its store before reading and fails with the blocked host named.

When a host is blocked:

1. Run `scripts/check.py` once and report the blocked hosts as it prints them. Do not retry with other tools, mirrors or another data source the user did not ask for.
2. Never estimate, simulate or fill in the values. A blocked network is not missing data.
3. Prepare everything that needs no network: the points file and the exact command.
4. Give the user two ways forward: allow the hosts in the agent's network settings (the change may only apply to a new session), or run the command on their own machine and share the output file.

## Rules

1. Open the one skill the question needs. Each is self-contained. Pass the same points and dates between skills.
2. To run a script, use `uv run`. It needs only `uv`, which fetches Python and the packages on first run. If `uv` is missing, give the user the one-line install from https://docs.astral.sh/uv/getting-started/installation/ and ask before installing.
3. Return the output path, source/store, grid, units, actual dates (or soil depths/statistic), retrieval date and relevant caveats. Values are gridded estimates for a cell, not station readings for a point.
4. Report points and variables with missing values, including partial date gaps. Missing coordinates, coverage limits and source masks are different from network or authentication failures; do not label every failure as water or fill gaps silently. For a network failure follow "Network".
5. Return daily data when a calling skill asked for it. Start hazard analysis only when the user asked for it.
6. Never invent a DOI, licence or date. Each skill ends with what to cite.

## Maps

Draw every map in the Equal Earth projection (EPSG:8857), the equal-area world map the UN General Assembly endorsed in September 2026 and the standard across maniva. Inputs and outputs stay in WGS84 (EPSG:4326); project only to draw. In Python, `gdf.to_crs("EPSG:8857")`; in R with ggplot2, `coord_sf(crs = sf::st_crs("EPSG:8857"), default_crs = sf::st_crs(4326))`. Interactive tile maps (leaflet, folium) stay in Web Mercator, which tile services require; say so in the caption.

## R users

The skills are Python. Run the script, then read the parquet output in R with `nanoparquet::read_parquet()` (small, no dependencies) or `arrow::read_parquet()`.
