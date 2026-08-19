import csv
import datetime
import logging
import shutil
from collections import defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Final
from uuid import UUID

import netCDF4
import numpy as np
from cftime import date2num
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
    # Cabauw, pluvioDC
    "Intensity_RT",
    "Accu_total_NRT",
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
            # Cabauw, pluvioDC
            "Intensity_RT": RATE,
            "Accu_total_NRT": AMOUNT,
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
        # First value is masked in Cabauw
        first_valid = np.nonzero(~ma.getmaskarray(data))[0][0]
        offset = 0
        for i in range(first_valid + 1, len(data)):
            if data[i] + offset < data[i - 1]:
                offset += data[i - 1]
            data[i] += offset
        data -= data[first_valid]
        self.nc.variables[AMOUNT][:] = data


def pluvio2nc(inpath: list[Path], outpath: Path, expected_date: datetime.date) -> None:
    raw_data: dict = defaultdict(list)
    for path in inpath:
        with open(path) as f:
            reader = csv.reader(f)
            _origin = next(reader)
            headers = next(reader)
            _units = next(reader)
            _process = next(reader)
            for line in reader:
                for key, value in zip(headers, line, strict=True):
                    if key == "TIMESTAMP":
                        raw_data[key].append(
                            datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                        )
                    elif key == "RECORD":
                        raw_data[key].append(int(value))
                    else:
                        raw_data[key].append(float(value))

    time = np.array(raw_data["TIMESTAMP"])
    is_valid = np.array([t.date() == expected_date for t in time])
    time, time_ind = np.unique(time[is_valid], return_index=True)
    rr = np.array(raw_data["Intensity"])[is_valid][time_ind]
    ra = np.array(raw_data["AccumTotalNRT"])[is_valid][time_ind]

    with netCDF4.Dataset(outpath, "w") as nc:
        nc.createDimension("time")

        time_var = nc.createVariable("time", "i4", "time")
        time_var.units = "seconds since 1970-01-01 00:00:00 +00:00"
        time_var[:] = date2num(time, time_var.units)

        rr_var = nc.createVariable("rain_rate", "f4", "time")
        rr_var.units = "mm h-1"
        rr_var[:] = rr

        ra_var = nc.createVariable("am_tot", "f4", "time")
        ra_var.units = "mm"
        ra_var[:] = ra
