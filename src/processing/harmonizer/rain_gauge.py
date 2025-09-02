import logging
import shutil
from tempfile import NamedTemporaryFile
from typing import Final
from uuid import UUID

import netCDF4
from cloudnetpy.instruments import Instrument, instruments
from numpy import ma

from processing.harmonizer import core

RATE: Final = "rainfall_rate"
AMOUNT: Final = "rainfall_amount"

CORRECT_UNITS = {
    "49ca09de-ca9a-4e3e-9258-9c91ed5683f8": {"rain_rate": "mm/h"},  # juelich pluvio
    "00a9fdae-6ac8-4028-97f5-d1dd5c171991": {
        "time": "seconds since 01/01/1970 00:00:00 +00:00",
        "rain_intensity": "mm/h",
    },  # maido pluvio
}

VALID_KEYS = (
    "time",
    "int_h",
    "am_tot",
    "rain_rate",
    "total_accum_NRT",
    "rain_intensity",
)


def harmonize_thies_pt_nc(data: dict) -> UUID:
    return _harmonize(data, instruments.THIES_PT)


def harmonize_pluvio_nc(data: dict) -> UUID:
    return _harmonize(data, instruments.PLUVIO2)


def _harmonize(data: dict, instrument: Instrument) -> UUID:
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
        gauge.copy_data(ind)
        gauge.mask_bad_data_values()
        gauge.fix_variable_names()
        gauge.fix_variable_attributes()
        gauge.to_ms1(RATE)
        gauge.to_m(AMOUNT)
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
    def mask_bad_data_values(self) -> None:
        for _, variable in self.nc.variables.items():
            variable[:] = ma.masked_invalid(variable[:])

    def fix_variable_attributes(self) -> None:
        for key in (RATE, AMOUNT):
            for attr in ("long_name", "standard_name", "comment"):
                self.harmonize_attribute(attr, (key,))

    def copy_data(
        self,
        time_ind: list,
    ) -> None:
        for key in VALID_KEYS:
            self._copy_variable(key, time_ind)

    def _copy_variable(self, key: str, time_ind: list) -> None:
        if key not in self.nc_raw.variables.keys():
            logging.debug(f"Key {key} not found from the source file.")
            return

        variable = self.nc_raw.variables[key]
        dtype = "f8" if key == "time" else "f4"
        fill_value = netCDF4.default_fillvals[dtype] if key != "time" else None
        var_out = self.nc.createVariable(
            key, dtype, "time", zlib=True, fill_value=fill_value
        )
        instrument_uuid = self.data["instrument"].uuid
        new_units = CORRECT_UNITS.get(str(instrument_uuid), {}).get(key)
        if new_units is not None:
            logging.info(f"Correcting units of '{key}' to {new_units}.")
            var_out.units = new_units
        else:
            var_out.units = getattr(variable, "units", "1")

        screened_data = self._screen_data(variable, time_ind)
        var_out[:] = screened_data

    def fix_variable_names(self) -> None:
        keymap = {
            "rain_rate": RATE,
            "int_h": RATE,
            "int_m": RATE,
            "rain_intensity": RATE,
            "total_accum_NRT": AMOUNT,
            "am_tot": AMOUNT,
        }
        self.fix_name(keymap)

    def fix_pt_jumps(self) -> None:
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
