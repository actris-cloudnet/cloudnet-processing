import netCDF4
import numpy as np

from .utils import cftime2datetime64

C_TO_K = 273.15
HPA_TO_PA = 100


def read_cl61(nc: netCDF4.Dataset) -> dict:
    grp = nc.groups["monitoring"]
    if hasattr(grp, "Timestamp"):
        measurements = {
            key: np.array([getattr(grp, key)])
            for key in grp.ncattrs()
            if key != "Timestamp"
        }
        dt = round(float(grp.Timestamp))
        measurements["time"] = np.array([dt], dtype="datetime64[s]")
    else:
        measurements = {key: value[:] for key, value in grp.variables.items()}
        measurements["time"] = cftime2datetime64(grp["time"])
    measurements["internal_temperature"] = measurements["internal_temperature"] + C_TO_K
    measurements["internal_pressure"] = measurements["internal_pressure"] * HPA_TO_PA
    measurements["laser_temperature"] = measurements["laser_temperature"] + C_TO_K
    if "transmitter_enclosure_temperature" in measurements:
        measurements["transmitter_enclosure_temperature"] = (
            measurements["transmitter_enclosure_temperature"] + C_TO_K
        )
    return measurements
