<!-- badges: start -->
[![ProjStatus](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)
[![Lifecycle](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
<!-- badges: end -->

## Climate data for on-farm trials and breeding

> Brayden Youngberg, Kauê de Sousa

Daily weather, elevation, administrative boundaries and climate hazards for points such as farms, trial sites and survey households, as agent skills and the code behind them. Built for breeders and the Climate Action team who need to know what stresses a site or season faces, with tricot trials or on their own.

This is the climate layer of [ai-ready-documentation](https://github.com/AgrDataSci/ai-ready-documentation), the AgrDataSci family of repositories for evidence, protocols, data and analysis. It is built from [template-repo-data-analysis](https://github.com/AgrDataSci/template-repo-data-analysis) and works on its own.

---

## Sources

| Source | What | Resolution | Access |
|---|---|---|---|
| CHIRPS v3 | Daily rainfall, 1981 to about a month ago, 60°S to 60°N | 0.05° | public, no key |
| CHIRTS-ERA5 | Daily Tmax and Tmin, 1981 to about a week ago, 60°S to 70°N | 0.05° | public, no key |
| AgERA5 | Daily temperature, rainfall, radiation, humidity, wind, reference ET, 1979 to about a week ago | 0.1° | free Copernicus CDS key |
| Copernicus DEM | Elevation | 90 m | public, no key |
| GAUL 2025 | District and province boundaries | vector | public, no key |
| World Bank CCKP | Precomputed CMIP6 hazard indices, 2020 to 2099 | 0.25° | public, no key |

Each source keeps its own licence and terms of use.

---

## Skills

One folder per skill under `skills/`. Each has a `SKILL.md` an agent reads and a `scripts/fetch.py` it runs. Every script takes a CSV with `id`, `lat`, `lon` and writes parquet with one row per point and day, so outputs join on `id` and `date`.

| Skill | What it does | Status |
|---|---|---|
| `chirps-rainfall` | Daily rainfall for points from CHIRPS v3 | ready |
| `chirts-temperature` | Daily Tmax and Tmin for points from CHIRTS-ERA5 | ready |
| `agera5-climate` | Daily weather for points from AgERA5 | planned |
| `elevation` | Elevation for points from the Copernicus 90 m DEM | planned |
| `admin-boundaries` | District and province for points, or points in a named area, from GAUL 2025 | planned |
| `climate-hazards` | Dry spells, hot days, heavy rain and other hazards from any daily series, per growth stage or season | planned |
| `future-climate` | How those hazards shift under CMIP6 scenarios, from CCKP | planned |

---

## Install

Scripts need only [`uv`](https://docs.astral.sh/uv/getting-started/installation/), which fetches Python and the packages on first run.

**Claude Code.** Symlink or copy the skill folders into `~/.claude/skills/` for every project, or into a project's `.claude/skills/` for that project alone.

```sh
git clone https://github.com/AgrDataSci/climate-data.git
mkdir -p ~/.claude/skills
ln -s "$PWD"/climate-data/skills/* ~/.claude/skills/
```

**Codex and Gemini CLI.** Same, into `~/.agents/skills/` or `~/.gemini/skills/`.

**Without an agent.** Run any script directly, e.g. `uv run skills/chirps-rainfall/scripts/fetch.py points.csv rain.parquet --start 2020-01-01`.

**R users.** Outputs are parquet, which `arrow` reads. The R route to the same sources is the [chirps](https://github.com/AgrDataSci/chirps) and [climatrends](https://github.com/AgrDataSci/climatrends) packages.

---

## Repository structure

```text
climate-data/
├── skills/          # One folder per skill: SKILL.md, scripts/, references/ where needed
├── tests/           # One smoke test per script
├── AGENTS.md        # Short instructions for AI agents
├── SKILL.md         # Index of the skills and the rules they share
├── llms.txt         # Entry point for AI tools that read the web
├── CITATION.cff     # Citation metadata used by GitHub and Zenodo
├── LICENSE          # MIT
└── README.md        # This file
```

---

## Cite

Cite with [CITATION.cff](CITATION.cff).

## License

Code is released under the [MIT License](LICENSE), with rights held by the International Plant Genetic Resources Institute (Bioversity International). Documentation and protocols are CC BY 4.0. Third-party data keep their original licences.
