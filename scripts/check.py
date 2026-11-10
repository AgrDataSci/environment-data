"""Can this machine reach what the skills need? Standard library only, so it runs where packages cannot be installed.

    python3 scripts/check.py [skill ...]

Prints one line per host and exits 1 if any is blocked. Run it first in a sandbox, or after a fetch fails to connect."""

import shutil
import sys
import urllib.error
import urllib.request
from urllib.parse import urlsplit

# host lists mirror HOSTS in each skill's scripts/fetch.py
NEEDS = {
    "packages": ["pypi.org", "files.pythonhosted.org"],  # uv installs the Python packages from here on first run
    "chirps-rainfall": ["dynamical-ucsb-chc-chirps.s3.us-west-2.amazonaws.com"],
    "chirts-temperature": ["huggingface.co"],
    "agera5-climate": ["arco.datastores.ecmwf.int"],
    "elevation": ["copernicus-dem-90m.s3.amazonaws.com"],
    "soilgrids-soil": ["files.isric.org"],
}
CHIRTS = "https://huggingface.co/datasets/aaguilar90/chirts-era5-daily/resolve/main/chirts-era5-daily.zarr/"
CHUNKS = ["Tmax/0.0.0", "Tmax/0/0/0", "Tmax/c/0/0/0"]  # the first chunk under the layouts Zarr uses


class Trail(urllib.request.HTTPRedirectHandler):
    """Remember each host a download is redirected to."""

    hosts: list[str] = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        self.hosts.append(urlsplit(newurl).hostname)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def blocked(host: str) -> str | None:
    """Why the host is out of reach, or None. Any HTTP answer, even 403 or 404, means the host is reachable."""
    try:
        urllib.request.urlopen(urllib.request.Request(f"https://{host}/", method="HEAD"), timeout=20)
    except urllib.error.HTTPError:
        return None
    except OSError as exc:
        return str(getattr(exc, "reason", exc))
    return None


def chirts_download_hosts() -> dict[str, str | None]:
    """Hugging Face serves data files from a second host. Follow one chunk to find it and test it."""
    opener = urllib.request.build_opener(Trail)
    for chunk in CHUNKS:
        Trail.hosts = []
        try:
            opener.open(urllib.request.Request(CHIRTS + chunk, headers={"Range": "bytes=0-0"}), timeout=20)
            return {h: None for h in Trail.hosts}
        except urllib.error.HTTPError:
            continue  # not this layout
        except OSError as exc:
            return {h: (str(getattr(exc, "reason", exc)) if h == Trail.hosts[-1] else None) for h in Trail.hosts}
    return {}


if __name__ == "__main__":
    skills = sys.argv[1:] or [s for s in NEEDS if s != "packages"]
    if unknown := set(skills) - set(NEEDS):
        sys.exit(f"unknown skill {sorted(unknown)}; choose from {', '.join(s for s in NEEDS if s != 'packages')}")
    print(f"uv: {shutil.which('uv') or 'not found, see https://docs.astral.sh/uv/getting-started/installation/'}")
    if sys.version_info < (3, 12):
        print(f"python {sys.version.split()[0]}: older than 3.12, so uv will also download Python from github.com")
    failed = []
    for need in ["packages", *skills]:
        results = {h: blocked(h) for h in NEEDS[need]}
        if need == "chirts-temperature" and not results["huggingface.co"]:
            results |= chirts_download_hosts()
        for host, why in results.items():
            print(f"{'BLOCKED' if why else 'ok':8}{need:20}{host}{f'  ({why})' if why else ''}")
            failed += [host] if why else []
    if failed:
        print(f"\nThis network blocks {len(failed)} host(s). Allow them, or run the fetch on a machine with open internet:")
        sys.exit("\n".join(failed))
    print("\nAll hosts reachable.")
