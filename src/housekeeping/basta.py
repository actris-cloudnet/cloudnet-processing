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
        if var != "time":
            data = measurements[var]
            data = ma.masked_where(np.isclose(data, -999.0, atol=1e-6), data)

        if "degree_celsius" in getattr(nc[var], "units", "").lower():
            measurements[var] = c2k(measurements[var])

    return measurements
