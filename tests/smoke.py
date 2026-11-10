"""Fetch two points from every live store and check the answers. Needs uv and open internet.

    python3 tests/smoke.py

Catches a store that moved, changed layout or stopped answering. AgERA5 runs only when CDSAPI_KEY is set."""

import csv
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).parents[1]
WINDOW = ["--date-col", "planting_date", "--days", "5"]  # 2 points x days 0 to 5
# skill: (arguments, columns that must hold values, rows expected)
JOBS = {
    "chirps-rainfall": (WINDOW, ["precip_mm"], 12),
    "chirts-temperature": (WINDOW, ["tmax_c", "tmin_c"], 12),
    "agera5-climate": (WINDOW, ["et0", "srad"], 12),
    "elevation": ([], ["elevation_m"], 2),
    "soilgrids-soil": (["--props", "clay", "--depths", "0-5cm", "--res", "1000"], ["clay_0_5cm"], 2),
}


def fetch(skill: str, args: list[str]) -> list[dict]:
    out = Path(tempfile.mkdtemp()) / f"{skill}.csv"
    script = ROOT / "skills" / skill / "scripts" / "fetch.py"
    subprocess.run(["uv", "run", str(script), str(ROOT / "tests" / "points.csv"), str(out), *args], check=True)
    return list(csv.DictReader(out.open()))


if __name__ == "__main__":
    failed = []
    for skill, (args, columns, n) in JOBS.items():
        if skill == "agera5-climate" and not os.environ.get("CDSAPI_KEY"):
            print(f"skipped {skill}: no CDSAPI_KEY")
            continue
        try:
            rows = fetch(skill, args)
            assert len(rows) == n, f"{len(rows)} rows, expected {n}"
            assert all(r[c] != "" for r in rows for c in columns), f"missing values in {columns}"
            print(f"ok {skill}: {rows[0]}")
        except (AssertionError, subprocess.CalledProcessError) as exc:
            failed.append(skill)
            print(f"FAILED {skill}: {exc}")
    rows = fetch("chirps-rainfall", ["--start", "2024-01-01", "--end", "2024-01-03"])  # the one-period path
    if len(rows) != 6:
        failed.append("chirps-rainfall one period")
    sys.exit(f"failed: {failed}" if failed else None)
