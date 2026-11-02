# Instructions for AI agents

This repository holds skills that fetch weather, climate, elevation and boundary data for points such as farms and trial sites. If a user shares it with you, use it as follows.

1. Read `SKILL.md` first. It says which skill answers which need and the rules every skill shares.
2. Open only the skill folder the question needs, under `skills/`. Each `SKILL.md` is self-contained and its `scripts/` file does the fetch.
3. Run scripts with `uv run`. Do not install anything else without the user's agreement.
4. Keep the data's caveats with the data. Say which source, grid and dates a result came from.
5. Never invent a DOI, licence or date. Each skill ends with what to cite.

This is the environment layer of [ai-ready-documentation](https://github.com/AgrDataSci/ai-ready-documentation). It works on its own and with the tricot skills in that suite.
