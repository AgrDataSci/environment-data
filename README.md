<!-- badges: start -->
[![ProjStatus](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)
[![Lifecycle](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
<!-- badges: end -->

<a href="https://github.com/AgrDataSci/maniva"><img src="docs/maniva-logo.svg" alt="Part of maniva" height="64" align="right"></a>

# Environment data for on-farm trials and breeding

> Brayden Youngberg, Kauê de Sousa

Daily weather, elevation and soil properties for points such as farms, trial sites and survey households, as agent skills and the code behind them. District lookups and climate hazards are planned. Built for breeders and the Climate Action team who need to know what a site or season is like, with tricot trials or on their own.

This is the environment layer of [maniva](https://github.com/AgrDataSci/maniva), the AgrDataSci open commons of evidence, protocols, data and analysis for agricultural research. It is built from [template-repo-data-analysis](https://github.com/AgrDataSci/template-repo-data-analysis) and works on its own.

---

## Sources

| Source | What | Resolution | Access |
|---|---|---|---|
| CHIRPS v3 | Daily rainfall, 1981 onward; latest date depends on store, 60°S to 60°N | 0.05° | public, no key |
| CHIRTS-ERA5 | Daily Tmax and Tmin, 1981 onward; latest date depends on store, 60°S to 70°N | 0.05° | public, no key |
| AgERA5 | Daily temperature, rainfall, radiation, humidity, wind, reference ET, 1979 onward; latest date depends on store | 0.1° | free Copernicus CDS key |
| Copernicus DEM | Elevation | 90 m | public, no key |
| SoilGrids 2.0 | Soil texture, carbon, nitrogen, pH, CEC, bulk density at six depths | 250 m | public, no key |

Each source keeps its own licence and terms of use.

---

## Skills

One folder per skill under `skills/`. Each has a `SKILL.md` an agent reads and a `scripts/fetch.py` it runs. Every script takes points as a CSV with `id`, `lat`, `lon` and writes parquet (or CSV if the output name ends in `.csv`), one row per point and day for weather and one row per point for elevation and soil, so weather outputs join on `id` and `date`, and elevation/soil outputs join on `id`. An agent builds that CSV from whatever form the user's points come in. Each script's `fetch()` function also works inside a Python pipeline, and each `SKILL.md` documents the source well enough to write your own reader, e.g. in R.

| Skill | What it does | Status |
|---|---|---|
| `chirps-rainfall` | Daily rainfall for points from CHIRPS v3 | ready |
| `chirts-temperature` | Daily Tmax and Tmin for points from CHIRTS-ERA5 | ready |
| `agera5-climate` | Daily weather for points from AgERA5 | ready |
| `elevation` | Elevation for points from the Copernicus 90 m DEM | ready |
| `soilgrids-soil` | Soil properties at standard depths for points from SoilGrids 2.0 | ready |
| `admin-boundaries` | District and province for points, or points in a named area, from GAUL 2025 | planned |
| `climate-hazards` | Dry spells, hot days, heavy rain and other hazards from any daily series, per growth stage or season | planned |
| `future-climate` | How those hazards shift under CMIP6 scenarios, from CCKP | planned |

---

## Install

Scripts need only [`uv`](https://docs.astral.sh/uv/getting-started/installation/), which fetches Python and the packages on first run.

**Claude Code.** Symlink or copy the skill folders into `~/.claude/skills/` for every project, or into a project's `.claude/skills/` for that project alone.

```sh
git clone https://github.com/AgrDataSci/environment-data.git
mkdir -p ~/.claude/skills
ln -s "$PWD"/environment-data/skills/* ~/.claude/skills/
```

**Codex and Gemini CLI.** Same, into `~/.agents/skills/` or `~/.gemini/skills/`.

**Without an agent.** Run any script directly, e.g. `uv run skills/chirps-rainfall/scripts/fetch.py points.csv rain.parquet --start 2020-01-01`.

**R users.** Run the scripts as above, then read the parquet outputs in R with `nanoparquet::read_parquet()`, a small package with no dependencies, or `arrow::read_parquet()` if you already use arrow.

---

## Cite

Cite with [CITATION.cff](CITATION.cff).

## License

Code is released under the [MIT License](LICENSE), with rights held by the International Plant Genetic Resources Institute (Bioversity International). Documentation and protocols are CC BY 4.0. Third-party data keep their original licences.
