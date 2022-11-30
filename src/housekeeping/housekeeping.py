import logging
import tempfile
from os import getenv
from pathlib import Path
from typing import Callable

import numpy.typing as npt
import toml
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from netCDF4 import Dataset as Dataset
from pandas import DataFrame
from rpgpy import read_rpg
from rpgpy.utils import decode_rpg_status_flags, rpg_seconds2datetime64

from .chm15k import read_chm15k
from .exceptions import UnsupportedFile
from .hatpro import HatproHkd, HatproHkdNc


def _handle_hatpro_hkd(src: bytes) -> DataFrame:
    with tempfile.NamedTemporaryFile() as f:
        f.write(src)
        hkd = HatproHkd(f.name)
    time = hkd.data["T"]
    return _make_df(time, hkd.data, get_config("hatpro_hkd"))


def _handle_hatpro_nc(src: bytes) -> DataFrame:
    with tempfile.NamedTemporaryFile() as f:
        f.write(src)
        hkd = HatproHkdNc(f.name)
    return _make_df(hkd.data["time"], hkd.data, get_config("hatpro_nc"))


def _handle_rpg_lv1(src: bytes) -> DataFrame:
    with tempfile.NamedTemporaryFile() as f:
        f.write(src)
        head, data = read_rpg(f.name)
    time = rpg_seconds2datetime64(data["Time"])
    data |= decode_rpg_status_flags(data["Status"])._asdict()
    return _make_df(time, data, get_config("rpg-fmcw-94_lv1"))


def _handle_chm15k_nc(src: bytes) -> DataFrame:
    with Dataset("dataset.nc", memory=src) as nc:
        measurements = read_chm15k(nc)
        return _make_df(measurements["time"], measurements, get_config("chm15k_nc"))


def get_reader(metadata: dict) -> Callable[[bytes], DataFrame] | None:
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


def write(df: DataFrame, metadata: dict) -> None:
    df["site_id"] = metadata["siteId"]
    df["instrument_id"] = metadata["instrumentId"]
    df["instrument_pid"] = metadata["instrumentPid"]
    with make_influx_client() as client:
        with client.write_api(write_options=SYNCHRONOUS) as write_client:
            write_client.write(
                **get_write_arg(),
                record=df,
                data_frame_measurement_name="housekeeping",
                data_frame_tag_columns=["site_id", "instrument_id", "instrument_pid"],
            )


def _make_df(
    time: npt.NDArray, measurements: dict[str, npt.NDArray], variables: dict[str, str]
) -> DataFrame:
    if len(time) == 0:
        raise UnsupportedFile("No housekeeping data found")
    data = {}
    nonexisting_variables = []
    for src_name, dest_name in variables.items():
        if src_name not in measurements:
            nonexisting_variables.append((src_name, dest_name))
            continue
        data[dest_name] = measurements[src_name]
    if not data:
        raise UnsupportedFile("No housekeeping data found")
    for src_name, dest_name in nonexisting_variables:
        logging.warning(f"Variable '{src_name}' not found (would be mapped to '{dest_name}')")
    return DataFrame(data, index=time)


def get_config(format_id: str) -> dict:
    src = Path(__file__).parent.joinpath("config.toml")
    return toml.load(src)["format"][format_id]["vars"]


def list_instruments() -> list[str]:
    src = Path(__file__).parent.joinpath("config.toml")
    return list(toml.load(src)["metadata"].keys())


def get_write_arg() -> dict:
    return {key: getenv(f"INFLUXDB_{key.upper()}") for key in ["bucket", "org"]}


def make_influx_client() -> InfluxDBClient:
    return InfluxDBClient(
        **{key: getenv(f"INFLUXDB_{key.upper()}") for key in ["url", "bucket", "token", "org"]}
    )
