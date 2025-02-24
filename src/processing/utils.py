import base64
import datetime
import gzip
import hashlib
import logging
import os
import shutil
from pathlib import Path
from typing import Literal

import netCDF4
import numpy as np
import requests
from numpy import ma
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import processing.version
from processing.config import Config

ErrorSource = Literal["data", "worker", "freeze-cronjob", "qc-cronjob"]


class Uuid:
    __slots__ = ["raw", "product", "volatile"]

    def __init__(self):
        self.raw: list = []
        self.product: str = ""
        self.volatile: str | None = None


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


def utctoday() -> datetime.date:
    return utcnow().date()


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def create_product_put_payload(
    full_path: str | os.PathLike,
    storage_service_response: dict,
    volatile: bool,
    product: str | None = None,
    site: str | None = None,
    date_str: str | None = None,
) -> dict:
    """Creates put payload for data portal."""
    with netCDF4.Dataset(full_path, "r") as nc:
        start_time, stop_time = _get_data_timestamps(nc)
        payload = {
            "product": product or nc.cloudnet_file_type,
            "site": site or nc.location.lower(),
            "measurementDate": date_str or f"{nc.year}-{nc.month}-{nc.day}",
            "format": _get_file_format(nc),
            "checksum": sha256sum(full_path),
            "volatile": volatile,
            "uuid": getattr(nc, "file_uuid", ""),
            "pid": getattr(nc, "pid", ""),
            "software": {"cloudnet-processing": get_data_processing_version()},
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
    return payload


def _get_file_format(nc: netCDF4.Dataset):
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
        msg = "Abort PUT to data portal: time vector not an array"
        raise MiscError(msg)
    model_types = get_product_types("evaluation") + ["model"]
    is_model_or_l3 = file_type in model_types
    delta_seconds = 1 if (is_model_or_l3 and ind != 0) else 0
    delta = datetime.timedelta(seconds=delta_seconds)
    return base + datetime.timedelta(hours=fraction_hour) - delta


def get_product_types(product_type: str | None = None) -> list[str]:
    """Returns Cloudnet product types."""
    products = get_from_data_portal_api("api/products", {"developer": True})
    if product_type is not None:
        products = [product for product in products if product_type in product["type"]]
    return [product["id"] for product in products]


def _get_last_proper_model_data_ind(nc: netCDF4.Dataset) -> int:
    data = nc.variables["temperature"][:]
    unmasked_rows = ~np.all(ma.getmaskarray(data), axis=1)
    return min(np.where(unmasked_rows)[0][-1] + 1, data.shape[0] - 1)


def get_data_processing_version() -> str:
    version_file = Path(os.path.abspath(processing.version.__file__))
    version: dict = {}
    with open(version_file) as f:
        exec(f.read(), version)
    return version["__version__"]


def add_global_attributes(
    full_path: str | os.PathLike, instrument_pid: str | None = None
):
    """Add cloudnet-processing package version to file attributes."""
    version = get_data_processing_version()
    with netCDF4.Dataset(full_path, "r+") as nc:
        nc.cloudnet_processing_version = version
        if instrument_pid:
            nc.instrument_pid = instrument_pid


def read_site_info(site_name: str) -> dict:
    """Reads site information from Cloudnet http API."""
    sites = get_from_data_portal_api("api/sites", {"developer": True})
    for site in sites:
        if site["id"] == site_name:
            site["id"] = site_name
            site["name"] = site.pop("humanReadableName")
            return site
    raise ValueError(f"Invalid site name: {site_name}")


def fetch_calibration(instrument_pid: str, date: datetime.date | str) -> dict | None:
    """Gets calibration factor."""
    session = make_session()
    if isinstance(date, str):
        date = datetime.date.fromisoformat(date)
    data_portal_url = _fetch_data_portal_url()
    url = f"{data_portal_url}/api/calibration"
    payload = {"instrumentPid": instrument_pid, "date": date.isoformat()}
    res = session.get(url, params=payload)
    return res.json() if res.ok else None


def read_main_conf() -> Config:
    """Reads config from env vars."""
    return Config(os.environ)


def sha256sum(filename: str | os.PathLike) -> str:
    """Calculates hash of file using sha-256."""
    return _calc_hash_sum(filename, "sha256")


def md5sum(filename: str | os.PathLike, is_base64: bool = False) -> str:
    """Calculates hash of file using md5."""
    return _calc_hash_sum(filename, "md5", is_base64)


def _calc_hash_sum(filename, method, is_base64: bool = False) -> str:
    hash_sum = getattr(hashlib, method)()
    with open(filename, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_sum.update(byte_block)
    if is_base64:
        return base64.encodebytes(hash_sum.digest()).decode("utf-8").strip()
    return hash_sum.hexdigest()


class MiscError(Exception):
    """Internal exception class."""

    def __init__(self, msg: str):
        self.message = msg
        super().__init__(self.message)


class RawDataMissingError(Exception):
    """Internal exception class."""

    def __init__(self, msg: str = "Missing raw data"):
        self.message = msg
        super().__init__(self.message)


class SkipTaskError(Exception):
    """Unable to complete task for an expected reason."""

    def __init__(self, msg: str):
        self.message = msg
        super().__init__(self.message)


def build_file_landing_page_url(uuid: str) -> str:
    """Returns file landing page url."""
    config = read_main_conf()
    base = config.dataportal_public_url
    return f"{base}/file/{uuid}"


def get_from_data_portal_api(
    end_point: str, payload: dict | None = None
) -> list | dict:
    """Reads from data portal API."""
    session = make_session()
    data_portal_url = _fetch_data_portal_url()
    url = f"{data_portal_url}/{end_point}"
    return session.get(url=url, params=payload).json()


def _fetch_data_portal_url() -> str:
    """Returns data portal url."""
    config = read_main_conf()
    return config.dataportal_url


class MyAdapter(HTTPAdapter):
    def __init__(self):
        retry_strategy = Retry(total=10, backoff_factor=0.1, status_forcelist=[524])
        super().__init__(max_retries=retry_strategy)

    def send(
        self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None
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


class RawApi:
    def __init__(
        self, cfg: Config | None = None, session: requests.Session | None = None
    ):
        if cfg is None:
            cfg = read_main_conf()
        if session is None:
            session = make_session()
        self.base_url = cfg.dataportal_url
        self.session = session

    def get_raw_file(self, uuid: str, fname: str) -> bytes:
        url = f"{self.base_url}/api/download/raw/{uuid}/{fname}"
        return self.session.get(url).content


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
