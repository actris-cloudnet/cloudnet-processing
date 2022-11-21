import logging
import re
import tempfile
from datetime import datetime
from os import getenv
from pathlib import Path
from typing import Dict, Union

import cftime
import netCDF4
import numpy.typing as npt
import toml
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from netCDF4 import Dataset as Dataset
from pandas import DataFrame
from rpgpy import read_rpg
from rpgpy.utils import rpg_seconds2date

from .exceptions import HousekeepingEmptyWarning
from .hatpro import HatproHkd


def hatprohkd2db(src: bytes, metadata: dict):
    cfg = get_config("hatpro_hkd")
    with tempfile.NamedTemporaryFile() as f:
        f.write(src)
        hkd = HatproHkd(f.name)
    time = hkd.data["T"]
    df = _make_df(time, hkd.data, cfg["vars"])
    df2db(df, metadata)


def _rpgtime2datetime(rpg_timestamp: int) -> datetime:
    year, month, day, hour, minute, sec = [int(t) for t in rpg_seconds2date(rpg_timestamp)]
    return datetime(year, month, day, hour, minute, sec)


def _rpg2df(src: Union[Path, bytes], cfg: dict) -> DataFrame:
    if isinstance(src, bytes):
        with tempfile.NamedTemporaryFile() as f:
            f.write(src)
            head, data = read_rpg(f.name)
    elif isinstance(src, Path):
        head, data = read_rpg(src)
    else:
        raise TypeError

    time = [_rpgtime2datetime(t) for t in data["Time"]]
    return _make_df(time, data, cfg["vars"])


def rpg2db(src: bytes, metadata: dict) -> None:
    cfg = get_config(metadata["instrumentId"] + "_lv1")
    df = _rpg2df(src, cfg)
    df2db(df, metadata)


def nc2db(src: Union[Path, bytes], metadata: dict) -> None:
    cfg = get_config(metadata["instrumentId"] + "_nc")
    df = _nc2df(src, cfg)
    df2db(df, metadata)


def get_config(format_id: str) -> dict:
    src = Path(__file__).parent.joinpath("config.toml")
    return toml.load(src)["format"][format_id]


def _nc2df(nc_src: Union[Path, bytes], cfg: dict) -> DataFrame:
    if isinstance(nc_src, Path):
        if not nc_src.is_file():
            raise FileNotFoundError(f"{nc_src} not found")
        nc = Dataset(nc_src)
    elif isinstance(nc_src, bytes):
        nc = Dataset("dataset.nc", memory=nc_src)
    else:
        raise TypeError("nc_src must have type Path or bytes")
    time = _nctime2datetime(nc["time"])
    measurements = {var: nc[var][:] for var in nc.variables.keys()}
    return _make_df(time, measurements, cfg["vars"])


def df2db(df: DataFrame, metadata: dict) -> None:
    if df.empty:
        raise HousekeepingEmptyWarning()
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


def _nctime2datetime(time: netCDF4.Variable) -> npt.NDArray:
    units = _fix_invalid_cf_time_unit(time.units)
    return cftime.num2pydate(time[:], units=units)


def _fix_invalid_cf_time_unit(unit: str) -> str:
    match_ = re.match(
        r"^(\w+) since (\d{1,2})\.(\d{1,2})\.(\d{4}), (\d{1,2}):(\d{1,2}):(\d{1,2})$", unit
    )
    if match_:
        _unit = match_.group(1)
        day = match_.group(2).zfill(2)
        month = match_.group(3).zfill(2)
        year = match_.group(4)
        hour = match_.group(5).zfill(2)
        minute = match_.group(6).zfill(2)
        sec = match_.group(7).zfill(2)
        new_unit = f"{_unit} since {year}-{month}-{day} {hour}:{minute}:{sec}"
        return new_unit
    return unit


def _make_df(
    time: npt.NDArray, measurements: Dict[str, npt.NDArray], variables: Dict[str, str]
) -> DataFrame:
    data = {}
    for src_name, dest_name in variables.items():
        if src_name not in measurements:
            logging.warning(f"Variable '{src_name}' not found (would be mapped to '{dest_name}')")
            continue
        data[dest_name] = measurements[src_name]
    return DataFrame(data, index=time)


def get_write_arg() -> dict:
    return {key: getenv(f"INFLUXDB_{key.upper()}") for key in ["bucket", "org"]}


def make_influx_client() -> InfluxDBClient:
    return InfluxDBClient(
        **{key: getenv(f"INFLUXDB_{key.upper()}") for key in ["url", "bucket", "token", "org"]}
    )
