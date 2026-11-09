---
name: elevation
description: Fetch Copernicus GLO-90 surface elevation in metres for farms, trial sites or other coordinates. Use for elevation covariates; no API key needed.
---

# Elevation from the Copernicus DEM

The Copernicus GLO-90 digital elevation model covers the globe on a 90 m grid, as one cloud-optimised GeoTIFF per 1° tile on public AWS. Open access, no key, nothing to download first. In tricot, elevation sits beside climate as a covariate in the Plackett-Luce trees that show where a variety's advantage holds, so a breeder placing their own farms in those trees needs it.

## Run

Prepare a CSV with a unique, non-empty `id` per point and WGS84 decimal-degree `lat`, `lon` (longitude −180 to 180). Preserve identifiers, including leading zeros. Convert known missing-value sentinels to blank coordinates; do not guess corrections or move sites to obtain data. The script retains missing coordinates as NaN and rejects invalid ranges.

Resolve `scripts/fetch.py` relative to this skill's directory, not the user's working directory. Use absolute paths for the script, input and output when working elsewhere. Example from the skill directory:

```sh
uv run scripts/fetch.py points.csv elevation.parquet
```

Needs only `uv` ([install](https://docs.astral.sh/uv/getting-started/installation/)); it fetches Python and the packages on first run. Output is one row per point: `id`, `elevation_m`. The script prints IDs left NaN because coordinates are missing, a tile is absent, or the source cell is nodata. Network and other read errors fail the run. `(0, 0)` is a valid coordinate, not an automatic missing-value sentinel.

## If you need to read the tiles yourself

Tiles are named by their south-west corner, so (-1.95, 30.06) is in `S02_00_E030_00`:

```
https://copernicus-dem-90m.s3.amazonaws.com/Copernicus_DSM_COG_30_S02_00_E030_00_DEM/Copernicus_DSM_COG_30_S02_00_E030_00_DEM.tif
```

- WGS84 lon/lat, values in metres, float32. A tile over open ocean returns 404; treat it as no elevation.
- Group points by tile and open each tile once. Farms cluster, so hundreds of points usually touch a few tiles, at about 0.3 s each.
- Set `GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR`, otherwise GDAL lists the bucket on every open.
- From R, `terra::rast("/vsicurl/<url>")` and `extract` work the same way.

## Caveats

- It is a surface model, so values include tree canopy and buildings. Do not assume a fixed difference from ground elevation, even on farmland. Sea inside a coastal tile reads 0 m.
- Phone GPS altitude recorded in surveys is often tens of metres off. Prefer the DEM and use the GPS value only as a check.

## Deliver

Return the output path, units, grid resolution, retrieval date and IDs without values. Identify the source as a digital **surface** model. See the [provider description and licence](https://registry.opendata.aws/copernicus-dem/).

## Cite

Copernicus DEM, GLO-90. European Space Agency, Sinergise (2021). https://doi.org/10.5270/ESA-c5d3d65
