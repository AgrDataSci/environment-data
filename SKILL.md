---
name: climate-data
description: Index of the climate-data skills. Use when a user wants weather, climate, elevation, administrative boundaries or climate hazards for points such as farms, trial sites or survey households, to pick the right skill and chain them.
---

# Climate data for points

Daily weather, elevation, boundaries and climate hazards for sets of points, as one skill per source. Every skill takes a CSV with `id`, `lat`, `lon` and writes parquet with one row per point and day, so outputs join on `id` and `date` and chain into `climate-hazards`.

## Which skill

| Need | Skill | Source | Status |
|---|---|---|---|
| Daily rainfall | `chirps-rainfall` | CHIRPS v3, 0.05°, 1981 to about a month ago | ready |
| Daily Tmax and Tmin | `chirts-temperature` | CHIRTS-ERA5, 0.05°, 1981 to about a week ago | planned |
| Radiation, humidity, wind, ET | `agera5-climate` | AgERA5, 0.1°, needs a free CDS key | planned |
| Elevation | `elevation` | Copernicus 90 m DEM | planned |
| District and province | `admin-boundaries` | GAUL 2025 | planned |
| Dry spells, hot days, heavy rain, GDD per season or stage | `climate-hazards` | any daily series | planned |
| How those hazards shift to 2099 | `future-climate` | World Bank CCKP, CMIP6 | planned |

## Rules

1. Open the one skill the question needs. Each is self-contained. Pass the same points file and dates between skills.
2. Run the skill's `scripts/` file with `uv run`. It needs only `uv`, which fetches Python and the packages on first run. If `uv` is missing, give the user the one-line install from https://docs.astral.sh/uv/getting-started/installation/ and ask before installing.
3. Say which source and dates you used and repeat the skill's caveats. Values are gridded estimates for a cell, not station readings for a point.
4. Report points that come back all NaN. They are at sea, on a lake or off the grid. Do not fill them in.
5. Return daily data when a calling skill asked for it. Start hazard analysis only when the user asked for it.
6. Never invent a DOI, licence or date. Each skill ends with what to cite.

## R users

The skills are Python. Outputs are parquet, which R reads with `arrow`. The R route to the same sources is the `chirps` and `climatrends` packages by Kauê de Sousa.
