import cftime
import netCDF4


def read_mira(nc: netCDF4.Dataset) -> dict:
    measurements = {var: nc[var][:] for var in nc.variables}
    measurements["time"] = cftime.num2pydate(
        nc["time"][:], units="seconds since 1970-01-01"
    ).astype("datetime64")
    return measurements
