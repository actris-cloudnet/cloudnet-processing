import netCDF4
import numpy as np

from .utils import cftime2datetime64

C_TO_K = 273.15
HPA_TO_PA = 100

# Table 1. Alert status values
# https://docs.vaisala.com/r/M212475EN-E/en-US/GUID-6FC267DF-263D-4831-84D3-6B528D1E7F26/GUID-E5331EBF-0E35-4AFE-96F4-0FC394900604
STATUS = [
    "0",  # 0 = OK
    "I",  # 1 = Indication
    "W",  # 2 = At least one warning active, no alarms
    "A",  # 3 = At least one alarm active
]


def read_cl61(nc: netCDF4.Dataset) -> dict:
    return _read_monitoring(nc.groups["monitoring"]) | _read_status(nc.groups["status"])


def _read_monitoring(grp: netCDF4.Group) -> dict:
    if hasattr(grp, "Timestamp"):
        data = {
            key: np.array([getattr(grp, key)])
            for key in grp.ncattrs()
            if key != "Timestamp"
        }
        dt = round(float(grp.Timestamp))
        data["time"] = np.array([dt], dtype="datetime64[s]")
    else:
        data = {key: value[:] for key, value in grp.variables.items()}
        data["time"] = cftime2datetime64(grp["time"])
    data["internal_temperature"] = data["internal_temperature"] + C_TO_K
    data["internal_pressure"] = data["internal_pressure"] * HPA_TO_PA
    data["laser_temperature"] = data["laser_temperature"] + C_TO_K
    if "transmitter_enclosure_temperature" in data:
        data["transmitter_enclosure_temperature"] = (
            data["transmitter_enclosure_temperature"] + C_TO_K
        )
    return data


def _read_status(grp: netCDF4.Group) -> dict:
    if hasattr(grp, "Timestamp"):
        data = {
            key: np.array([STATUS.index(getattr(grp, key)[-1])])
            for key in grp.ncattrs()
            if key != "Timestamp"
        }
        dt = round(float(grp.Timestamp))
        data["time"] = np.array([dt], dtype="datetime64[s]")
    else:
        data = {key: value[:] for key, value in grp.variables.items()}
        data["time"] = cftime2datetime64(grp["time"])
    return data
