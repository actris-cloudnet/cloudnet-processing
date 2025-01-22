import logging
import shutil
from tempfile import NamedTemporaryFile
from typing import Final

import netCDF4
from cloudnetpy.instruments import Instrument, instruments
from numpy import ma

from processing.harmonizer import core

RATE: Final = "rainfall_rate"
AMOUNT: Final = "rainfall_amount"


def harmonize_thies_pt_nc(data: dict) -> str:
    vars = ("time", "int_h", "am_tot")
    return _harmonize(vars, data, instruments.THIES_PT)


def harmonize_pluvio_nc(data: dict) -> str:
    vars = ("time", "rain_rate", "total_accum_NRT")
    return _harmonize(vars, data, instruments.PLUVIO2)


def _harmonize(vars: tuple, data: dict, instrument: Instrument):
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
        gauge.copy_data(vars, ind)
        gauge.mask_bad_data_values()
        gauge.fix_variable_names()
        gauge.fix_variable_attributes()
        gauge.convert_rainfall_rate()
        gauge.convert_rainfall_amount()
        if instrument == instruments.THIES_PT:
            gauge.fix_pt_jumps()
        gauge.normalize_rainfall_amount()
        uuid = gauge.add_uuid()
        gauge.add_global_attributes("rain-gauge", instrument)
        gauge.add_date()
        gauge.convert_time()
        gauge.add_geolocation()
        gauge.add_history("rain-gauge")
    if "output_path" not in data:
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


class RainGaugeNc(core.Level1Nc):
    def mask_bad_data_values(self):
        for _, variable in self.nc.variables.items():
            variable[:] = ma.masked_invalid(variable[:])

    def fix_variable_attributes(self):
        for key in (RATE, AMOUNT):
            for attr in ("long_name", "standard_name", "comment"):
                self.harmonize_attribute(attr, (key,))

    def copy_data(
        self,
        keys: tuple,
        time_ind: list,
    ):
        for key in keys:
            self._copy_variable(key, time_ind)

    def _copy_variable(self, key: str, time_ind: list):
        if key not in self.nc_raw.variables.keys():
            logging.warning(f"Key {key} not found from the source file.")
            return

        variable = self.nc_raw.variables[key]
        dtype = "f8" if key == "time" else "f4"
        fill_value = netCDF4.default_fillvals[dtype] if key != "time" else None
        var_out = self.nc.createVariable(
            key, dtype, "time", zlib=True, fill_value=fill_value
        )
        var_out.units = getattr(variable, "units", "1")
        screened_data = self._screen_data(variable, time_ind)
        var_out[:] = screened_data

    def fix_variable_names(self):
        keymap = {
            "rain_rate": RATE,
            "int_h": RATE,
            "int_m": RATE,
            "total_accum_NRT": AMOUNT,
            "am_tot": AMOUNT,
        }
        self.fix_name(keymap)

    def convert_rainfall_rate(self):
        """Converts rainfall rate to m s-1."""
        units = self.nc.variables[RATE].units.lower()
        match units:
            case "m/s" | "m s-1" | "m / s":
                factor = 1.0
            case "mm/h" | "mm/hour" | "mm h-1" | "mm / h" | "mm / hour":
                factor = 1e-3 / 3600
            case "mm/min" | "mm min-1" | "mm / min":
                factor = 1e-3 / 60
            case "mm/s" | "mm s-1" | "mm / s":
                factor = 1e-3
            case _:
                raise ValueError(f"Unknown units: {units}")
        self.nc.variables[RATE][:] *= factor
        self.nc.variables[RATE].units = "m s-1"
        logging.info(f"Converted {RATE} from {units} to m s-1.")

    def convert_rainfall_amount(self):
        """Converts rainfall amount to m."""
        units = self.nc.variables[AMOUNT].units.lower()
        match units:
            case "m":
                factor = 1.0
            case "mm":
                factor = 1e-3
            case _:
                raise ValueError(f"Unknown units: {units}")
        self.nc.variables[AMOUNT][:] *= factor
        self.nc.variables[AMOUNT].units = "m"
        logging.info(f"Converted {AMOUNT} from {units} to m.")

    def fix_pt_jumps(self):
        """Fixes suspicious jumps from a valid value to single 0-value and back in Thies PT data."""
        data = self.nc.variables[AMOUNT][:]
        for i in range(1, len(data) - 1):
            if data[i] == 0 and data[i - 1] > 0 and data[i + 1] > 0:
                data[i] = data[i + 1]
        self.nc.variables[AMOUNT][:] = data

    def normalize_rainfall_amount(self) -> None:
        """Copied from Cloudnetpy."""
        data = self.nc.variables[AMOUNT][:]
        offset = 0
        for i in range(1, len(data)):
            if data[i] + offset < data[i - 1]:
                offset += data[i - 1]
            data[i] += offset
        data -= data[0]
        self.nc.variables[AMOUNT][:] = data
