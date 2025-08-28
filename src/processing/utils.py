import datetime
import gzip
import logging
import shutil
from pathlib import Path
from typing import Literal
from uuid import UUID

import netCDF4
import numpy as np
import requests
from cloudnet_api_client.utils import sha256sum
from numpy import ma
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from processing.config import Config
from processing.version import __version__ as cloudnet_processing_version

ErrorSource = Literal["data", "worker", "freeze-cronjob", "qc-cronjob"]


class Uuid:
    __slots__ = ["raw", "product", "volatile"]

    def __init__(self) -> None:
        self.raw: list[str] = []
        self.product: str = ""
        self.volatile: str | None = None


class MiscError(Exception):
    """Internal exception class."""

    def __init__(self, msg: str) -> None:
        self.message = msg
        super().__init__(self.message)


class RawDataMissingError(Exception):
    """Internal exception class."""

    def __init__(self, msg: str = "Missing raw data") -> None:
        self.message = msg
        super().__init__(self.message)


class SkipTaskError(Exception):
    """Unable to complete task for an expected reason."""

    def __init__(self, msg: str) -> None:
        self.message = msg
        super().__init__(self.message)


class MyAdapter(HTTPAdapter):
    def __init__(self) -> None:
        retry_strategy = Retry(
            total=10, backoff_factor=0.1, status_forcelist=[504, 524]
        )
        super().__init__(max_retries=retry_strategy)

    def send(  # noqa: ANN201
        self,
        request,  # noqa: ANN001
        stream=False,  # noqa: ANN001
        timeout=None,  # noqa: ANN001
        verify=True,  # noqa: ANN001
        cert=None,  # noqa: ANN001
        proxies=None,  # noqa: ANN001
    ):
        if timeout is None:
            timeout = 120
        return super().send(request, stream, timeout, verify, cert, proxies)


def make_session() -> requests.Session:
    adapter = MyAdapter()
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    return http


def utctoday() -> datetime.date:
    return utcnow().date()


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def send_slack_alert(
    config: Config,
    exception: Exception,
    source: ErrorSource,
    log: str | None = None,
    date: str | None = None,
    site: str | None = None,
    product: str | None = None,
    model: str | None = None,
) -> None:
    """Sends notification to Slack."""
    if not config.slack_api_token or not config.slack_channel_id:
        logging.warning("Slack is not configured: no notification will be sent!")
        return

    match source:
        case "data":
            label = ":desktop_computer: Data processing"
        case "worker":
            label = ":construction_worker: Worker"
        case "freeze-cronjob":
            label = ":cold_face: Freeze cronjob"
        case "qc-cronjob":
            label = ":first_place_medal: Yesterday's QC cronjob"
        case unknown_source:
            label = f":interrobang: Unknown error source ({unknown_source})"

    padding = " " * 7
    msg = f"*{label}*\n\n"

    for name, var in zip(
        ("Site", "Date", "Product", "Model"), (site, date, product, model)
    ):
        if var is not None:
            msg += f"*{name}:* {var}{padding}"

    timestamp = datetime.datetime.now(datetime.timezone.utc)
    msg += f"*Time:* {timestamp:%Y-%m-%d %H:%M:%S}\n\n"
    msg += f"*Error:* {exception}"

    payload = {
        "content": log or "(empty log)",
        "channels": config.slack_channel_id,
        "title": "Full log",
        "initial_comment": msg,
    }

    session = make_session()
    r = session.post(
        "https://slack.com/api/files.upload",
        data=payload,
        headers={"Authorization": f"Bearer {config.slack_api_token}"},
    )
    r.raise_for_status()
    body = r.json()
    if not body["ok"]:
        logging.fatal(f"Failed to send Slack notification: {body.text}")


def add_global_attributes(full_path: Path, instrument_pid: str | None = None) -> None:
    """Add cloudnet-processing package version to file attributes."""
    with netCDF4.Dataset(full_path, "r+") as nc:
        nc.cloudnet_processing_version = cloudnet_processing_version
        if instrument_pid:
            nc.instrument_pid = instrument_pid


def print_info(
    uuid: Uuid,
    volatile: bool,
    patch: bool,
    upload: bool,
    qc_result: str | None = None,
) -> None:
    if not upload:
        action = "Kept existing file"
    elif patch:
        action = "Patched existing file"
    elif not volatile:
        action = "Created new version"
    else:
        action = "Replaced volatile file" if uuid.volatile else "Created volatile file"

    link = build_file_landing_page_url(uuid.product)
    qc_str = f" QC: {qc_result.upper()}" if qc_result else ""
    logging.info(f"{action}: {link}{qc_str}")


def build_file_landing_page_url(uuid: str | UUID) -> str:
    """Returns file landing page url."""
    config = Config()
    base = config.dataportal_public_url
    return f"{base}/file/{uuid}"


def unzip_gz_file(path_in: Path) -> Path:
    if path_in.suffix != ".gz":
        return path_in
    path_out = path_in.parent / path_in.stem
    logging.debug(f"Decompressing {path_in} to {path_out}")
    with gzip.open(path_in, "rb") as file_in:
        with open(path_out, "wb") as file_out:
            shutil.copyfileobj(file_in, file_out)
    path_in.unlink()
    return path_out


def create_product_put_payload(
    full_path: Path,
    storage_service_response: dict,
    site: str,
    volatile: bool,
    patch: bool,
) -> dict:
    """Creates put payload for data portal."""
    with netCDF4.Dataset(full_path, "r") as nc:
        start_time, stop_time = _get_data_timestamps(nc)
        payload = {
            "product": nc.cloudnet_file_type,
            "site": site,
            "measurementDate": f"{nc.year}-{nc.month}-{nc.day}",
            "format": _get_file_format(nc),
            "checksum": sha256sum(full_path),
            "volatile": volatile,
            "patch": patch,
            "uuid": getattr(nc, "file_uuid", ""),
            "pid": getattr(nc, "pid", ""),
            "software": {"cloudnet-processing": cloudnet_processing_version},
            "startTime": start_time,
            "stopTime": stop_time,
            **storage_service_response,
        }
        if instrument_pid := getattr(nc, "instrument_pid", None):
            payload["instrumentPid"] = instrument_pid
        if source_uuids := getattr(nc, "source_file_uuids", None):
            payload["sourceFileIds"] = [
                uuid.strip() for uuid in source_uuids.split(",")
            ]
        if version := getattr(nc, "cloudnetpy_version", None):
            payload["software"]["cloudnetpy"] = version
        if version := getattr(nc, "mwrpy_version", None):
            payload["software"]["mwrpy"] = version
        if version := getattr(nc, "doppy_version", None):
            payload["software"]["doppy"] = version
        if version := getattr(nc, "voodoonet_version", None):
            payload["software"]["voodoonet"] = version
        if version := getattr(nc, "model_munger_version", None):
            payload["software"]["model-munger"] = version
        if version := getattr(nc, "ceilopyter_version", None):
            payload["software"]["ceilopyter"] = version
    return payload


def _get_file_format(nc: netCDF4.Dataset) -> str:
    """Returns netCDF file format."""
    file_format = nc.file_format.lower()
    if "netcdf4" in file_format:
        return "HDF5 (NetCDF4)"
    if "netcdf3" in file_format:
        return "NetCDF3"
    raise RuntimeError("Unknown file type")


def _get_data_timestamps(nc: netCDF4.Dataset) -> tuple[str, str]:
    """Returns first and last timestamps."""
    t1 = _get_datetime(nc, ind=0)
    t2 = _get_datetime(nc, ind=-1)
    time_format = "%Y-%m-%dT%H:%M:%S%z"
    return t1.strftime(time_format), t2.strftime(time_format)


def _get_datetime(nc: netCDF4.Dataset, ind: int) -> datetime.datetime:
    y, m, d = int(nc.year), int(nc.month), int(nc.day)
    tz = datetime.timezone.utc
    base = datetime.datetime(y, m, d, tzinfo=tz)
    file_type = getattr(nc, "cloudnet_file_type", None)
    if file_type == "model" and ind == -1:
        ind = _get_last_proper_model_data_ind(nc)
    try:
        fraction_hour = float(nc.variables["time"][:][ind])
    except IndexError:
        msg = "Time vector not an array"
        raise SkipTaskError(msg)
    return base + datetime.timedelta(hours=fraction_hour)


def _get_last_proper_model_data_ind(nc: netCDF4.Dataset) -> int:
    data = nc.variables["temperature"][:]
    unmasked_rows = ~np.all(ma.getmaskarray(data), axis=1)
    return min(np.where(unmasked_rows)[0][-1] + 1, data.shape[0] - 1)
