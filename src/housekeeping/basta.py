import netCDF4
import numpy as np
from numpy import ma

from housekeeping.utils import cftime2datetime64


def c2k(celsius: float) -> float:
    return celsius + 273.15


def read_basta(nc: netCDF4.Dataset) -> dict:
    measurements = {var: nc[var][:] for var in nc.variables}
    measurements["time"] = cftime2datetime64(nc["time"])
    for var in measurements:
        data = measurements[var]
        if var != "time":
            is_invalid = np.isclose(data, -999.0, atol=1e-6)
            data = ma.masked_where(is_invalid, data)
        if "degree_celsius" in getattr(nc[var], "units", "").lower():
            data = c2k(data)
        measurements[var] = data
    return measurements
