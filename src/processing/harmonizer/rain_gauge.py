import logging
import shutil
from tempfile import NamedTemporaryFile

import netCDF4
from cloudnetpy.instruments import instruments
from numpy import ma

from processing.harmonizer import core


def harmonize_thies_pt_nc(data: dict) -> str:
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
        gauge = RainGaugeNc(nc_raw, nc, data)
        ind = gauge.get_valid_time_indices()
        gauge.nc.createDimension("time", len(ind))
        gauge.copy_data(("time", "int_m", "am_tot", "am_red"), ind)
        gauge.mask_bad_data_values()
        gauge.fix_variable_names()
        gauge.clean_global_attributes()
        gauge.convert_units()
        for key in (
            "rainfall_rate",
            "rainfall_amount",
        ):
            gauge.harmonize_standard_attributes(key)
        gauge.harmonize_attribute("comment", ("rainfall_amount",))
        gauge.fix_long_names()
        uuid = gauge.add_uuid()
        gauge.add_global_attributes("rain-gauge", instruments.THIES_PT)
        gauge.add_date()
        gauge.convert_time()
        gauge.add_geolocation()
        gauge.add_history("rain-gauge")
        gauge.fix_jumps_in_pt()
        gauge.normalize_rainfall_amount()
    if "output_path" not in data:
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


def harmonize_pluvio_nc(data: dict) -> str:
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
        gauge = RainGaugeNc(nc_raw, nc, data)
        ind = gauge.get_valid_time_indices()
        gauge.nc.createDimension("time", len(ind))
        gauge.copy_data(
            ("time", "rain_rate", "r_accum_RT", "r_accum_NRT", "total_accum_NRT"), ind
        )
        gauge.mask_bad_data_values()
        gauge.fix_variable_names()
        gauge.convert_units()
        for key in (
            "rainfall_rate",
            "rainfall_amount",
        ):
            gauge.harmonize_standard_attributes(key)
        gauge.harmonize_attribute("comment", ("rainfall_amount",))
        gauge.fix_long_names()
        uuid = gauge.add_uuid()
        gauge.add_global_attributes("rain-gauge", instruments.PLUVIO2)
        gauge.add_date()
        gauge.convert_time()
        gauge.add_geolocation()
        gauge.add_history("rain-gauge")
        gauge.normalize_rainfall_amount()
    if "output_path" not in data:
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


class RainGaugeNc(core.Level1Nc):
    def mask_bad_data_values(self):
        for _, variable in self.nc.variables.items():
            variable[:] = ma.masked_invalid(variable[:])

    def copy_data(
        self,
        keys: tuple,
        time_ind: list,
    ):
        for key in keys:
            self._copy_variable(key, time_ind)
        self._copy_global_attributes()

    def _copy_variable(self, key: str, time_ind: list):
        if key not in self.nc_raw.variables.keys():
            logging.warning(f"Key {key} not found from the source file.")
            return

        variable = self.nc_raw.variables[key]
        dtype = "f8" if key == "time" and "int" in str(variable.dtype) else "f4"
        fill_value = netCDF4.default_fillvals[dtype] if key != "time" else None
        var_out = self.nc.createVariable(
            key, dtype, "time", zlib=True, fill_value=fill_value
        )
        self._copy_variable_attributes(variable, var_out)
        screened_data = self._screen_data(variable, time_ind)
        var_out[:] = screened_data

    @staticmethod
    def _copy_variable_attributes(var_in: netCDF4.Variable, var_out: netCDF4.Variable):
        skip = ("_FillValue", "_Fill_Value", "description", "comment")
        for attr in var_in.ncattrs():
            if attr not in skip:
                setattr(var_out, attr, getattr(var_in, attr))

    def fix_variable_names(self):
        keymap = {
            "rain_rate": "rainfall_rate",
            "int_m": "rainfall_rate",
            "total_accum_NRT": "rainfall_amount",
            "am_tot": "rainfall_amount",
            "am_red": "r_accum_RT",
        }
        self.fix_name(keymap)

    def fix_long_names(self):
        keymap = {
            "r_accum_RT": "Real time accumulated rainfall",
            "r_accum_NRT": "Near real time accumulated rainfall",
        }
        self.fix_attribute(keymap, "long_name")

    def convert_units(self):
        mm_to_m = 0.001
        mmh_to_ms = mm_to_m / 3600
        self.nc.variables["rainfall_rate"].units = "m s-1"
        self.nc.variables["rainfall_rate"][:] *= mmh_to_ms
        for key in ("r_accum_RT", "r_accum_NRT", "rainfall_amount"):
            if key not in self.nc.variables:
                continue
            self.nc.variables[key].units = "m"
            self.nc.variables[key][:] *= mm_to_m

    def fix_jumps_in_pt(self):
        """Fixes suspicious jumps from a valid value to single 0-value and back in Thies PT data."""
        data = self.nc.variables["rainfall_amount"][:]
        for i in range(1, len(data) - 1):
            if data[i] == 0 and data[i - 1] > 0 and data[i + 1] > 0:
                data[i] = data[i + 1]
        self.nc.variables["rainfall_amount"][:] = data

    def normalize_rainfall_amount(self) -> None:
        """Copied from Cloudnetpy."""
        data = self.nc.variables["rainfall_amount"][:]
        offset = 0
        for i in range(1, len(data)):
            if data[i] + offset < data[i - 1]:
                offset += data[i - 1]
            data[i] += offset
        data -= data[0]
        self.nc.variables["rainfall_amount"][:] = data
