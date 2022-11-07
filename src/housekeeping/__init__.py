import pathlib
from pathlib import Path
from pdb import set_trace as db
from netCDF4 import Dataset
from pandas import DataFrame
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient
from typing import Union
from influxdb_client.client.write_api import SYNCHRONOUS
import numpy as np
import numpy.typing as npt
from os import getenv
import toml
import re


def nc2db(src: Union[Path,bytes], site_id: str, instrument_id: str, instrument_pid: str) -> None:
    cfg = get_config()
    df = nc2df(src, cfg)
    df2db(df, site_id, instrument_id, instrument_pid)

def get_config() -> dict: 
    src = Path(__file__).parent.joinpath("config.toml")
    cfgs = toml.load(src)
    return cfgs["global"]
    
def nc2df(nc_src: Union[Path,bytes], cfg: dict) -> dict:
    if isinstance(nc_src, Path):
        if not nc_src.is_file():
            raise FileNotFoundError(f"{nc_src} not found")
        nc = Dataset(nc_src)
    elif isinstance(nc_src, bytes):
        nc = Dataset("dataset.nc", memory=nc_src)
    else:
        raise TypeError("nc_src must have type Path or bytes")
    time = nctime2datetime(nc["time"])
    measurements = {}
    nc_keys = nc.variables.keys()
    for var in cfg["vars"]:
        if var in nc_keys:
            measurements[var] = nc[var][:]
    return DataFrame(measurements, index=time) 

def df2db(df,site_id: str, instrument_id: str, instrument_pid: str) -> None:
    if df.empty:
        return
    df["site_id"] = site_id
    df["instrument_id"] = instrument_id
    df["instrument_pid"] = instrument_pid
    with make_influx_client() as client:
        with client.write_api(write_options=SYNCHRONOUS) as write_client:
            (write_client
                .write(
                    **get_write_arg(),
                    record=df,
                    data_frame_measurement_name="housekeeping",
                    data_frame_tag_columns=["site_id", "instrument_id", "instrument_pid"]
                )
            )


def nctime2datetime(nct: Dataset) -> npt.NDArray:
    re_t = re.compile(r"^seconds since (\d{4})-(\d{2})-(\d{2}) 00:00:00(?:\.000(?: 00:00)?)?$")
    m = re_t.match(nct.units)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))
        day = int(m.group(3))
        base = datetime(year,month,day,0,0,0)
        data = [base + timedelta(seconds=s) for s in nct[:]]
        mask = nct[:].mask
        return  np.ma.MaskedArray(data=data, mask=mask)
    else:
        raise ValueError(f"Unexpected time units: {nct.units}")

def get_write_arg():
    args = {key: getenv(f"INFLUXDB_{key.upper()}") 
            for key in ["bucket", "org"]}
    return args

def make_influx_client():
    args = {key: getenv(f"INFLUXDB_{key.upper()}") 
            for key in ["url", "bucket", "token", "org"]}
    return InfluxDBClient(**args)

