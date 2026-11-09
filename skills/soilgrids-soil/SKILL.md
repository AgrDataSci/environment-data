---
name: soilgrids-soil
description: Fetch SoilGrids 2.0 soil properties and prediction intervals at standard depths for land points, at 250 m or as 1 km averages. Use for soil covariates such as texture, carbon, pH or bulk density; no API key needed.
---

# SoilGrids 2.0 soil properties for points

SoilGrids is ISRIC's global map of soil properties, predicted at 250 m from about 240,000 soil profiles with machine learning. Each property comes at six depths with a mean and a 90% prediction interval. ISRIC also publishes 1 km averages of the mean. This reads the files ISRIC serves openly. No key, nothing to download first.

## Run

Prepare a CSV with a unique, non-empty `id` per point and WGS84 decimal-degree `lat`, `lon` (longitude −180 to 180). Preserve identifiers, including leading zeros. Convert known missing-value sentinels to blank coordinates; do not guess corrections or move sites to obtain data. The script retains missing coordinates as NaN and rejects invalid ranges.

Resolve `scripts/fetch.py` relative to this skill's directory, not the user's working directory. Use absolute paths for the script, input and output when working elsewhere. Example from the skill directory:

```sh
uv run scripts/fetch.py points.csv soil.parquet
uv run scripts/fetch.py points.csv soil-q05.parquet --props clay,soc,phh2o --depths 0-5cm,5-15cm,15-30cm,30-60cm --stat Q0.05
uv run scripts/fetch.py points.csv soil-1km.parquet --res 1000
```

Needs only `uv` ([install](https://docs.astral.sh/uv/getting-started/installation/)); it fetches Python and the packages on first run. Output is one row per point and one column per property and depth, e.g. `id`, `clay_0_5cm`, `soc_15_30cm`. The default is 9 properties at the three topsoil depths (0–30 cm). The script prints any point with no data.

Expect about 20 to 30 s for 1,000 points and the default 27 layers. Each layer costs a 6 MB index download plus one small read per 450×9-pixel strip the points touch, so clustered farms are cheap and extra layers cost more than extra points.

Use `--res 1000` when point locations are uncertain by more than a few hundred metres, such as tricot sites, which are displaced up to 1.5 km for privacy. A 1 km average then describes the area the farm is in better than one 250 m cell. It has the mean only, so prediction intervals stay at 250 m.

## Properties and units

Values are converted from SoilGrids' stored integers to the units below.

| Name | Property | Unit |
|---|---|---|
| `clay`, `sand`, `silt` | Texture fractions of the fine earth | % |
| `soc` | Soil organic carbon | g/kg |
| `nitrogen` | Total nitrogen | g/kg |
| `phh2o` | pH in water | pH |
| `cec` | Cation exchange capacity at pH 7 | cmol(c)/kg |
| `bdod` | Bulk density of the fine earth | kg/dm³ |
| `cfvo` | Coarse fragments | vol % |
| `ocd` | Organic carbon density | kg/m³ |
| `ocs` | Organic carbon stock, 0–30 cm only | kg/m² |

Depths are `0-5cm`, `5-15cm`, `15-30cm`, `30-60cm`, `60-100cm`, `100-200cm`. `ocs` always produces a single `ocs_0_30cm` column, regardless of `--depths`. `--stat` is `mean` (default), `Q0.5` (median), or `Q0.05` and `Q0.95` for the bounds of the 90% prediction interval.

## If you need to read the files yourself

```
https://files.isric.org/soilgrids/latest/data/{prop}/{prop}_{depth}_{stat}.vrt
https://files.isric.org/soilgrids/latest/data_aggregated/1000m/{prop}/{prop}_{depth}_mean_1000.tif
```

- The 1 km files are Cloud Optimized GeoTIFFs, one per property and depth, in the same projection and scaling as the 250 m data.

- Each VRT indexes GeoTIFF tiles. ISRIC also provides overview files ([layer documentation](https://docs.isric.org/globaldata/soilgrids/SoilGrids_faqs_01.html)); this helper samples points, while regional reads need a separate strategy.
- The projection is Interrupted Goode Homolosine, not lon/lat. Reproject the points before sampling.
- Values are scaled integers. Divide by 100 for `bdod` and `nitrogen` and by 10 for the rest. Nodata is -32768.
- Tile grids differ between layers, so open each layer's own VRT.
- Set `GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR`, otherwise GDAL lists a directory on every tile open.
- From R, `terra::rast("/vsicurl/<url>")` and `extract` work the same way.

## Caveats

- These are model predictions for a 250 m or 1 km cell, not measurements at the farm. Local differences from management, erosion or fertiliser are not in them. Say so when reporting values.
- Prediction intervals are wide, often wider than the differences between nearby farms. When the decision depends on the value, report the interval as well (`--stat Q0.05` and `Q0.95`).
- Urban areas, water, permanent ice and bare rock are masked, so points in towns come back NaN. Report them. Do not fill them in.
- Topsoil predictions (0–30 cm) are better supported than deep layers, which rest on fewer profiles.

## Deliver

Return the output path, properties, depths, units, statistic, resolution, source and retrieval date, plus missing-value counts. Save mean, lower-bound and upper-bound runs to separate files: their column names are identical and do not encode the statistic. If merging them, suffix columns with the statistic.

## Cite

Poggio L et al. (2021). SoilGrids 2.0: producing soil information for the globe with quantified spatial uncertainty. SOIL 7, 217–240. https://doi.org/10.5194/soil-7-217-2021. Data: ISRIC – World Soil Information, CC BY 4.0, https://files.isric.org/soilgrids/latest/
