---
name: elevation
description: Get ground elevation in metres above sea level for points such as farms, trial sites or survey households, anywhere on Earth, from the Copernicus 90 m DEM. No API key needed. Use when a user wants altitude or elevation for coordinates or a dataset of locations, or needs it as a covariate alongside climate.
---

# Elevation from the Copernicus DEM

The Copernicus GLO-90 digital elevation model covers the globe on a 90 m grid, as one cloud-optimised GeoTIFF per 1° tile on public AWS. Open access, no key, nothing to download first. In tricot, elevation sits beside climate as a covariate in the Plackett-Luce trees that show where a variety's advantage holds, so a breeder placing their own farms in those trees needs it.

## Run

Put the points in a CSV with columns `id`, `lat`, `lon`, then

```sh
uv run scripts/fetch.py points.csv elevation.parquet
```

Needs only `uv` ([install](https://docs.astral.sh/uv/getting-started/installation/)); it fetches Python and the packages on first run. Output is one row per point: `id`, `elevation_m`. The script prints any point left NaN: open water has no tile, and blank or `0,0` coordinates are skipped.

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

- It is a surface model, so values include tree canopy and buildings. Over farmland that is within a few metres of ground. Sea inside a coastal tile reads 0 m.
- Phone GPS altitude recorded in surveys is often tens of metres off. Prefer the DEM and use the GPS value only as a check.

## Cite

Copernicus DEM, GLO-90. European Space Agency, Sinergise (2021). https://doi.org/10.5270/ESA-c5d3d65
