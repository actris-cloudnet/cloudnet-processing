import netCDF4

from .utils import cftime2datetime64

C_TO_K = 273.15
HPA_TO_PA = 100


def read_cl61(nc: netCDF4.Dataset) -> dict:
    grp = nc.groups["monitoring"]
    measurements = {var: grp[var][:] for var in grp.variables.keys()}
    measurements["time"] = cftime2datetime64(grp["time"])
    measurements["internal_temperature"] = measurements["internal_temperature"] + C_TO_K
    measurements["internal_pressure"] = measurements["internal_pressure"] * HPA_TO_PA
    measurements["laser_temperature"] = measurements["laser_temperature"] + C_TO_K
    measurements["transmitter_enclosure_temperature"] = (
        measurements["transmitter_enclosure_temperature"] + C_TO_K
    )
    return measurements
