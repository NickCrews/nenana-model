from __future__ import annotations

import logging
import warnings
from pathlib import Path
from typing import Iterable

import cdsapi
from cdsapi.api import Result

logger = logging.getLogger(__name__)


def _ignore_insecure_request_warning():
    # ignore .venv/lib/python3.11/site-packages/urllib3/connectionpool.py:1103:
    # InsecureRequestWarning: Unverified HTTPS request is being made to host
    # 'cds.climate.copernicus.eu'. Adding certificate verification is strongly advised.
    # See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#tls-warnings
    warnings.filterwarnings(
        "ignore",
        module="urllib3.connectionpool",
        message=r".*copernicus.*",
    )


_ignore_insecure_request_warning()


def make_client() -> cdsapi.Client:
    return cdsapi.Client(
        wait_until_complete=False,  # return immediately with the request ID
        delete=False,  # don't delete the request when the client is garbage collected
    )


class Task:
    def __init__(
        self,
        request_id: str,
        *,
        client: cdsapi.Client | None = None,
    ) -> None:
        if client is None:
            client = make_client()
        self.request_id = request_id
        self.client = client
        self._result = Result(client=self.client, reply=None)
        self.update()

    @classmethod
    def from_result(cls, result: Result) -> Task:
        request_id = result.reply["request_id"]
        # yuck this is a nasty hack
        client = result.error.__self__
        return cls(request_id, client=client)

    @classmethod
    def from_file(cls, path: str | Path) -> Task:
        with open(path) as f:
            request_id = f.read().strip()
        return cls(request_id)

    def to_file(self, path: str | Path) -> None:
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


# https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels?tab=form
# https://pypi.org/project/cdsapi/
def begin_export(year: int) -> Task:
    """Tell the ERA5 API to begin exporting data for the given year.

    Get your user ID (UID) and API key from the CDS portal at
    https://cds.climate.copernicus.eu/user and write it to ~/.cdsapirc in the form
    url: https://cds.climate.copernicus.eu/api/v2
    key: <UID>:<API key>
    verify: 0
    """
    days = [f"{i:02}" for i in range(1, 32)]
    times = [f"{i:02}:00" for i in range(24)]
    options = {
        "product_type": "reanalysis",
        "format": "grib",
        "year": str(year),
        "area": [64.6, -149.1, 64.4, -148.9],
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
    _ignore_insecure_request_warning()
    result = make_client().retrieve("reanalysis-era5-single-levels", options)
    return Task.from_result(result)


def download_years(
    years: Iterable[int] | None = None,
    directory: str | Path | None = None,
    *,
    log_level: str | int = "INFO",
) -> list[tuple[int, str]]:
    """Download ERA5 data for the given years to the given directory.

    This function will skip years for which the data already exists in the directory.

    This returns a list of tuples, each containing the year and the status of the task.
    """
    logging.basicConfig(level=log_level)
    if years is None:
        years = range(1940, 2025)
    if directory is None:
        directory = "data/era5"
    years = list(years)
    base = Path(directory).absolute()
    statuses = []
    for year in years:
        out_path = base / f"{year}.grib"
        if out_path.exists():
            logger.info(f"Skipping {year} because {out_path} exists")
            continue
        cache_path = base / f"{year}.requestid"
        if not cache_path.exists():
            logger.info(f"Beginning export for {year}")
            task = begin_export(year)
            task.to_file(cache_path)
        else:
            logger.info(f"Loading task for {year} from {cache_path}")
            task = Task.from_file(cache_path)
        if task.status == "completed":
            logger.info(f"Task for {year} is complete. Downloading!")
            task.download(out_path)
        else:
            logger.info(f"Task for {year} is not complete.")
        statuses.append((year, task.status))
    return statuses


if __name__ == "__main__":
    download_years()
