# Instructions for AI agents

This repository holds skills that fetch weather, elevation and soil data for points such as farms and trial sites. If a user shares it with you, use it as follows.

1. Read `SKILL.md` first. It says which skill answers which need and the rules every skill shares.
2. Open only the skill folder the question needs, under `skills/`. Each `SKILL.md` is self-contained and its `scripts/` file does the fetch.
3. Run scripts with `uv run`. Do not install anything else without the user's agreement. If a script cannot reach its data store, or `uv` cannot install packages, follow "Network" in `SKILL.md`: run `python3 scripts/check.py`, name the blocked hosts, and hand the user the exact command. Never estimate values or switch source.
4. Keep the data's caveats with the data. Say which source, grid and dates a result came from.
5. Never invent a DOI, licence or date. Each skill ends with what to cite.
6. Write maniva and tricot in lowercase, always, including headings, table cells and the start of a sentence. Never write "Maniva" or "Tricot". tricot stands for triadic comparison of technology options. Quote published titles exactly as printed, even where they capitalise tricot or expand it as "triadic comparisons of technologies".
7. Draw every map in the Equal Earth projection (EPSG:8857). Keep coordinates in WGS84 (EPSG:4326) for storage and calculations, and project only to draw. Interactive tile maps stay in Web Mercator. See "Maps" in `SKILL.md`.

This is the environment layer of [maniva](https://github.com/AgrDataSci/maniva), the AgrDataSci open commons of evidence, protocols, data and analysis for agricultural research. It works on its own and with the tricot layers of maniva.
