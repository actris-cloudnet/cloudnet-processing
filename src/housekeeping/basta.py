import netCDF4

from housekeeping.utils import cftime2datetime64


def c2k(celsius: float) -> float:
    return celsius + 273.15


def read_basta(nc: netCDF4.Dataset) -> dict:
    measurements = {var: nc[var][:] for var in nc.variables}
    measurements["time"] = cftime2datetime64(nc["time"])
    for var in measurements:
        if "degree_celsius" in getattr(nc[var], "units", "").lower():
            measurements[var] = c2k(measurements[var])

    return measurements
