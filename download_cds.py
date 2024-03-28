from __future__ import annotations

import logging
import warnings
from pathlib import Path
from typing import Any

import cdsapi
from cdsapi.api import Result

logger = logging.getLogger(__name__)


class Task:
    """
    A data export task using the Copernicus Climate Data Store (CDS) API.

    Get your user ID (UID) and API key from the CDS portal at
    https://cds.climate.copernicus.eu/user and write it to `~/.cdsapirc` in the form
    url: https://cds.climate.copernicus.eu/api/v2
    key: <UID>:<API key>
    verify: 0

    See https://pypi.org/project/cdsapi/
    and
    https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels?tab=form
    for more information.
    """

    def __init__(self, request_id: str, *, client: cdsapi.Client | None = None) -> None:
        if client is None:
            client = _make_client()
        self.request_id = request_id
        self._result = Result(client=client, reply=None)
        self.update()

    @classmethod
    def new(cls, options: dict[str, Any]) -> Task:
        """Create a new export Task with the CDS API."""
        _ignore_insecure_request_warning()
        client = _make_client()
        result = client.retrieve("reanalysis-era5-single-levels", options)
        request_id = result.reply["request_id"]
        return cls(request_id, client=client)

    @classmethod
    def from_file(cls, path: str | Path) -> Task:
        """Restore a Task from a file containing the request ID."""
        with open(path) as f:
            request_id = f.read().strip()
        return cls(request_id)

    def to_file(self, path: str | Path) -> None:
        """Save the request ID to a file."""
        with open(path, "w") as f:
            f.write(self.request_id)

    @property
    def status(self) -> str:
        """One of "queued", "running", "completed", "failed"."""
        return self._result.reply["state"]

    def update(self) -> None:
        """Poll the API to get the latest status of the task."""
        self._result.update(self.request_id)

    def download(self, path: str) -> None:
        """Download the (assumed to be completed) result of the task."""
        if "location" not in self._result.reply:
            raise ValueError("Task is not complete")
        self._result.download(path)

    def __repr__(self) -> str:
        return f"Task(request_id={self.request_id}, status={self.status})"


def _make_client() -> cdsapi.Client:
    return cdsapi.Client(
        wait_until_complete=False,  # return immediately with the request ID
        delete=False,  # don't delete the request when the client is garbage collected
    )


def _ignore_insecure_request_warning():
    # The vanilla CDS client raises all these spurious warnings about insecure requests:
    # .venv/lib/python3.11/site-packages/urllib3/connectionpool.py:1103:
    # InsecureRequestWarning: Unverified HTTPS request is being made to host
    # 'cds.climate.copernicus.eu'. Adding certificate verification is strongly advised.
    # See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#tls-warnings
    warnings.filterwarnings(
        "ignore",
        module="urllib3.connectionpool",
        message=r".*copernicus.*",
    )


_ignore_insecure_request_warning()


def begin_export() -> Task:
    days = [f"{i:02}" for i in range(1, 32)]
    times = [f"{i:02}:00" for i in range(24)]
    years = [str(i) for i in range(1940, 2025)]
    options = {
        "product_type": "reanalysis",
        "format": "grib",
        "area": [64.6, -149.1, 64.4, -148.9],
        "year": years,
        "month": ["01", "02", "03", "04", "05"],
        "day": days,
        "time": times,
        "variable": [
            "mean_snowmelt_rate",
            "surface_latent_heat_flux",
            "surface_net_solar_radiation",
            "surface_net_thermal_radiation",
            "surface_sensible_heat_flux",
        ],
    }
    return Task.new(options)


def main(
    path: str | Path | None = None,
    *,
    log_level: str | int = "INFO",
) -> str:
    logging.basicConfig(level=log_level)
    if path is None:
        path = "data/era5.grib"
    path = Path(path).absolute()
    if path.exists():
        logger.info(f"Skipping because {path} exists")
        return
    cache_path = path.with_suffix(".requestid")
    if not cache_path.exists():
        logger.info("Beginning export")
        task = begin_export()
        task.to_file(cache_path)
    else:
        logger.info(f"Loading taskfrom {cache_path}")
        task = Task.from_file(cache_path)
    if task.status == "completed":
        logger.info("Task is complete. Downloading!")
        task.download(path)
    else:
        logger.info("Task is not complete.")
    return task.status


if __name__ == "__main__":
    main()
