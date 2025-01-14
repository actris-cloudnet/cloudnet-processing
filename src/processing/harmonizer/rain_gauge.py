import shutil
from tempfile import NamedTemporaryFile

import netCDF4
from cloudnetpy.instruments import instruments

from processing.harmonizer import core


def harmonize_rain_gauge_file(data: dict) -> str:
    if "output_path" not in data:
        temp_file = NamedTemporaryFile()
    with (
        netCDF4.Dataset(data["full_path"], "r") as nc_raw,
        netCDF4.Dataset(
            data["output_path"] if "output_path" in data else temp_file.name,
            "w",
            format="NETCDF4_CLASSIC",
        ) as nc,
    ):
        gauge = core.Level1Nc(nc_raw, nc, data)
        ind = gauge.get_valid_time_indices()
        gauge.copy_file_contents(
            keys=("time", "rain_rate", "r_accum_RT", "r_accum_NRT", "total_accum_NRT"),
            time_ind=ind,
        )
        gauge.mask_bad_data_values()
        gauge.harmonize_attribute("units", ("latitude", "longitude", "altitude"))
        gauge.fix_name({"rain_rate": "rainfall_rate"})
        uuid = gauge.add_uuid()
        gauge.add_global_attributes("rain-gauge", instruments.PLUVIO)
        gauge.add_date()
        gauge.convert_time()
        gauge.add_geolocation()
        gauge.add_history("rain-gauge")
    if "output_path" not in data:
        shutil.copy(temp_file.name, data["full_path"])
    return uuid
