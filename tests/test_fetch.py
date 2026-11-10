"""Offline tests on synthetic stores. No network.

    uv run --no-project --with pytest --with xarray --with "zarr>=3" --with icechunk --with dask --with fsspec \
        --with aiohttp --with pandas --with pyarrow --with rasterio --with numpy pytest tests
"""

import argparse
import importlib.util
import inspect
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

ROOT = Path(__file__).parents[1]
WEATHER = ["chirps-rainfall", "chirts-temperature", "agera5-climate"]
SKILLS = [*WEATHER, "elevation", "soilgrids-soil"]


def load(path: Path):
    spec = importlib.util.spec_from_file_location(path.parent.parent.name.replace("-", "_") + "_" + path.stem, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


MODS = {s: load(ROOT / "skills" / s / "scripts" / "fetch.py") for s in SKILLS}
# per skill: latitude and longitude names, store variables, north-to-south grid, value column, output value on day 10 of the year
STORES = {
    "chirps-rainfall": ("latitude", "longitude", ["precipitation_surface"], True, "precip_mm", 86400.0),
    "chirts-temperature": ("lat", "lon", ["Tmax", "Tmin"], True, "tmax_c", 1.0),
    "agera5-climate": ("latitude", "longitude", [v[0] for v in MODS["agera5-climate"].VARS.values()], False, "et0", 1.0),
}
POINTS = pd.DataFrame({
    "id": ["a", "b", "c", "d"],
    "lat": [5.1, -3.3, 5.2, np.nan],  # d has no coordinates
    "lon": [2.2, 36.7, 2.3, 2.0],
    "date": ["2023-02-01", "2023-06-15", None, "2023-03-01"],  # c has no date
})


def store(lat: str, lon: str, variables: list[str], descending: bool) -> xr.Dataset:
    """A year on a 0.5 degree grid over Africa. Every cell holds a tenth of the day of the year, so a window is easy to check."""
    time = pd.date_range("2023-01-01", "2023-12-31")
    lats = np.arange(-9.75, 10, 0.5)[:: -1 if descending else 1]
    lons = np.arange(-9.75, 45, 0.5)
    doy = np.broadcast_to(np.arange(1, 366, dtype="float32")[:, None, None] / 10, (365, len(lats), len(lons)))
    return xr.Dataset({v: (("time", lat, lon), doy.copy()) for v in variables}, coords={"time": time, lat: lats, lon: lons})


@pytest.fixture(params=WEATHER)
def weather(request, monkeypatch):
    mod, (lat, lon, variables, descending, column, scale) = MODS[request.param], STORES[request.param]
    monkeypatch.setattr(mod, "open_store", lambda: store(lat, lon, variables, descending))
    monkeypatch.setattr(mod, "reachable", lambda hosts: None)
    return mod, column, scale


def test_shared_helpers_are_identical():
    """Each skill is self-contained, so the helpers are copied. This catches a copy that drifts."""
    shared = {"validate_points": SKILLS, "reachable": SKILLS, "select_daily": WEATHER, "windows": WEATHER, "clip": WEATHER, "cli_points": WEATHER}
    for name, skills in shared.items():
        assert len({inspect.getsource(getattr(MODS[s], name)) for s in skills}) == 1, name


def test_check_knows_every_host():
    check = load(ROOT / "scripts" / "check.py")
    assert {s: check.NEEDS[s] for s in SKILLS} == {s: MODS[s].HOSTS for s in SKILLS}


def test_window_per_point(weather):
    mod, column, scale = weather
    out = mod.fetch(POINTS, days=10)
    assert list(out.columns[:3]) == ["id", "date", "day"]
    assert set(out.id) == {"a", "b", "d"}  # c has no date, so no window
    assert out.groupby("id").size().eq(11).all()
    a = out[out.id == "a"]
    assert list(a.day) == list(range(11)) and str(a.date.iloc[0]) == "2023-02-01"
    np.testing.assert_allclose(a[column], np.arange(32, 43) / 10 * scale, rtol=1e-5)  # 1 February is day 32
    assert a.cell_lat.iloc[0] == pytest.approx(5.25) and a.cell_lon.iloc[0] == pytest.approx(2.25)
    assert out[out.id == "b"][column].iloc[0] == pytest.approx(16.6 * scale, rel=1e-5)  # 15 June
    assert out[out.id == "d"][column].isna().all()  # no coordinates: rows stay, values are NaN


def test_one_period_for_all_points(weather):
    mod, column, scale = weather
    out = mod.fetch(POINTS, "2023-03-01", "2023-03-05")
    assert "day" not in out and len(out) == 5 * len(POINTS)
    assert out[out.id == "a"][column].iloc[0] == pytest.approx(6.0 * scale, rel=1e-5)


def test_window_outside_the_store_fails(weather):
    mod, _, _ = weather
    late = POINTS.assign(date="2023-12-28")
    with pytest.raises(ValueError, match="within"):  # never shorten a requested period silently
        mod.fetch(late, days=10)
    with pytest.raises(ValueError, match="give start"):
        mod.fetch(POINTS)


def test_overlapping_windows_share_a_read():
    mod = MODS["chirts-temperature"]
    pts = pd.DataFrame({"id": list("abcd"), "lat": [5.1, 5.2, 5.3, -20.0], "lon": 2.0, "date": ["2023-02-01", "2023-02-05", "2023-08-01", "2023-02-01"]})
    groups = sorted(mod.windows(mod.validate_points(pts), 10), key=lambda g: (g[1], -len(g[0])))
    assert [list(g.id) for g, _, _ in groups] == [["a", "b"], ["d"], ["c"]]  # d sits in another spatial chunk
    assert groups[0][1:] == (pd.Timestamp("2023-02-01"), pd.Timestamp("2023-02-15"))


@pytest.mark.parametrize("skill", SKILLS)
def test_points_are_validated(skill):
    validate = MODS[skill].validate_points
    with pytest.raises(ValueError, match="missing"):
        validate(pd.DataFrame({"id": ["a"], "latitude": [1.0], "longitude": [2.0]}))
    with pytest.raises(ValueError, match="unique"):
        validate(pd.DataFrame({"id": ["a", "a"], "lat": [1.0, 1.0], "lon": [2.0, 2.0]}))
    with pytest.raises(ValueError, match="outside"):
        validate(pd.DataFrame({"id": ["a"], "lat": [36.7], "lon": [-183.3]}))


def test_cli_takes_one_mode(tmp_path):
    mod = MODS["chirps-rainfall"]
    POINTS.rename(columns={"date": "planting_date"}).to_csv(tmp_path / "points.csv", index=False)
    parser = argparse.ArgumentParser()
    args = dict(points=tmp_path / "points.csv", start=None, end=None, date_col="planting_date", days=10)
    assert "date" in mod.cli_points(parser, argparse.Namespace(**args))
    for wrong in ({"days": None}, {"start": "2023-01-01"}, {"date_col": None}, {"date_col": "sowing"}):
        with pytest.raises(SystemExit):
            mod.cli_points(parser, argparse.Namespace(**(args | wrong)))
