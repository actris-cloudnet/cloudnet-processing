from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Callable, Iterable

import numpy as np
import numpy.typing as npt
import toml
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from netCDF4 import Dataset
from numpy import ma
from rpgpy import read_rpg
from rpgpy.utils import decode_rpg_status_flags, rpg_seconds2datetime64

from data_processing.utils import RawApi

from .chm15k import read_chm15k
from .exceptions import HousekeepingException, UnsupportedFile
from .hatpro import HatproHkd, HatproHkdNc


def process_record(record: dict, raw_api: RawApi, db: Database):
    try:
        reader = get_reader(record)
        filename = record["filename"]
        uuid = record["uuid"]
        if reader is None:
            logging.info(f"Skipping: {filename}")
            return
        logging.info(f"Processing housekeeping data: {filename}")
        filebytes = raw_api.get_raw_file(uuid, filename)
        points = reader(filebytes, record)
        db.write(points)
    except UnsupportedFile as err:
        logging.warning(f"Unable to process file: {err}")
    except KeyboardInterrupt as err:
        raise err
    except Exception as err:
        raise HousekeepingException from err


def _handle_hatpro_hkd(src: bytes, metadata: dict) -> list[Point]:
    with tempfile.NamedTemporaryFile() as f:
        f.write(src)
        hkd = HatproHkd(f.name)
    time = hkd.data["T"]
    return _make_points(time, hkd.data, get_config("hatpro_hkd"), metadata)


def _handle_hatpro_nc(src: bytes, metadata: dict) -> list[Point]:
    with tempfile.NamedTemporaryFile() as f:
        f.write(src)
        hkd = HatproHkdNc(f.name)
    return _make_points(hkd.data["time"], hkd.data, get_config("hatpro_nc"), metadata)


def _handle_rpg_lv1(src: bytes, metadata: dict) -> list[Point]:
    with tempfile.NamedTemporaryFile() as f:
        f.write(src)
        _, data = read_rpg(f.name)
    time = rpg_seconds2datetime64(data["Time"])
    data |= decode_rpg_status_flags(data["Status"])._asdict()
    return _make_points(time, data, get_config("rpg-fmcw-94_lv1"), metadata)


def _handle_chm15k_nc(src: bytes, metadata: dict) -> list[Point]:
    with Dataset("dataset.nc", memory=src) as nc:
        measurements = read_chm15k(nc)
        return _make_points(
            measurements["time"], measurements, get_config("chm15k_nc"), metadata
        )


def get_reader(metadata: dict) -> Callable[[bytes, dict], list[Point]] | None:
    instrument_id = metadata["instrumentId"]
    filename = metadata["filename"].lower()

    if instrument_id == "hatpro":
        if filename.endswith(".nc"):
            return _handle_hatpro_nc
        if filename.endswith(".hkd"):
            return _handle_hatpro_hkd

    if instrument_id == "rpg-fmcw-94" and filename.endswith(".lv1"):
        return _handle_rpg_lv1

    if instrument_id == "chm15k" and filename.endswith(".nc"):
        return _handle_chm15k_nc

    return None


class Database:
    def __init__(self) -> None:
        self.client = InfluxDBClient(
            url=os.environ["INFLUXDB_URL"],
            token=os.environ["INFLUXDB_TOKEN"],
            org=os.environ["INFLUXDB_ORG"],
        )
        self.bucket = os.environ["INFLUXDB_BUCKET"]
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def write(self, points: Iterable[Point]):
        self.write_api.write(bucket=self.bucket, record=points)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.write_api.close()
        self.client.close()


def _make_points(
    time: npt.NDArray,
    measurements: dict[str, npt.NDArray],
    variables: dict[str, str],
    metadata: dict,
) -> list[Point]:
    data = {}
    missing_variables = []
    for src_name, dest_name in variables.items():
        if src_name not in measurements:
            missing_variables.append((src_name, dest_name))
            continue
        data[dest_name] = measurements[src_name]
    if not data:
        raise UnsupportedFile("No housekeeping data found")
    for src_name, dest_name in missing_variables:
        msg = f"Variable '{src_name}' not found"
        if src_name != dest_name:
            msg += f" (would be mapped to '{dest_name}')"
        logging.warning(msg)

    timestamps = time.astype("datetime64[s]")
    date = np.datetime64(metadata["measurementDate"], "s")
    pad_hours = 1
    lower_limit = date - np.timedelta64(pad_hours, "h")
    upper_limit = date + np.timedelta64(24 + pad_hours, "h")
    valid_timestamps = (timestamps >= lower_limit) & (timestamps < upper_limit)
    if np.count_nonzero(valid_timestamps) == 0:
        raise UnsupportedFile("No housekeeping data found")

    points = []
    for i, timestamp in enumerate(timestamps):
        if not valid_timestamps[i]:
            continue
        fields = {
            key: values[i]
            for key, values in data.items()
            if not ma.is_masked(values[i])
        }
        if not fields:
            continue
        point = Point.from_dict(
            {
                "measurement": "housekeeping",
                "tags": {
                    "site_id": metadata["siteId"],
                    "instrument_id": metadata["instrumentId"],
                    "instrument_pid": metadata["instrumentPid"],
                },
                "fields": fields,
                "time": timestamp.astype(int),
            },
            WritePrecision.S,
        )
        points.append(point)

    return points


def get_config(format_id: str) -> dict:
    src = Path(__file__).parent.joinpath("config.toml")
    return toml.load(src)["format"][format_id]["vars"]


def list_instruments() -> list[str]:
    src = Path(__file__).parent.joinpath("config.toml")
    return list(toml.load(src)["metadata"].keys())
