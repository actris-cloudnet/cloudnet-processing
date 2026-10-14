import shutil
from tempfile import NamedTemporaryFile
from uuid import UUID

import netCDF4
from cloudnetpy.instruments import instruments
from numpy import ma

from processing.harmonizer import core


def harmonize_mira_file(data: dict) -> UUID:
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
        mira = MiraNc(nc_raw, nc, data)
        valid_ind = mira.get_valid_time_indices()
        mira.copy_file(valid_ind)
        mira.copy_mira_data(valid_ind)
        mira.fix_names()
        mira.clean_variable_attributes(("comment", "C0"))
        mira.clean_global_attributes()
        mira.add_geolocation()
        mira.add_height()
        mira.add_date()
        mira.convert_time()
        mira.add_global_attributes("radar", instruments.MIRA35)
        uuid = mira.add_uuid()
        mira.add_history("radar")
        mira.fix_long_names()
        mira.fix_standard_names()
        mira.fix_units()
        mira.mask_invalid_width()
    if "output_path" not in data:
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


class MiraNc(core.Level1Nc):
    def copy_file(self, valid_ind: list) -> None:
        keys = ("time", "range", "frequency", "NyquistVelocity")
        self.copy_file_contents(keys, valid_ind)

    def copy_mira_data(self, valid_ind: list) -> None:
        keys = ("Z", "v", "width", "ldr")
        for key in keys:
            src = self.nc_raw[key]
            fill_value = netCDF4.default_fillvals["f4"]
            dst = self.nc.createVariable(
                key, "f4", src.dimensions, zlib=True, fill_value=fill_value
            )
            attr = {
                k: src.getncattr(k)
                for k in src.ncattrs()
                if k not in ("_FillValue", "missing_value")
            }
            dst.setncatts(attr)
            data = src[valid_ind]
            dst[:] = ma.masked_equal(data, -999)

    def mask_invalid_width(self) -> None:
        var = self.nc["width"]
        var[:] = ma.masked_greater(var[:], 100)

    def fix_names(self) -> None:
        keymap = {
            "Z": "Zh",
            "frequency": "radar_frequency",
            "NyquistVelocity": "nyquist_velocity",
        }
        self.fix_name(keymap)

    def add_height(self) -> None:
        self.nc.renameVariable("range", "height")
        self.copy_variable("range")
        self.nc.variables["height"][:] += self.data["site_meta"]["altitude"]

    def fix_long_names(self) -> None:
        keymap = {
            "height": "Height above mean sea level",
            "range": "Range from instrument",
            "Zh": "Radar reflectivity factor",
            "v": "Doppler velocity",
            "width": "Spectral width",
            "ldr": "Linear depolarisation ratio",
            "radar_frequency": "Radar transmit frequency",
            "nyquist_velocity": "Nyquist velocity",
        }
        self.fix_attribute(keymap, "long_name")

    def fix_standard_names(self) -> None:
        keymap = {
            "height": "height_above_mean_sea_level",
        }
        self.fix_attribute(keymap, "standard_name")

    def fix_units(self) -> None:
        keymap = {
            "Zh": "dBZ",
            "ldr": "dB",
            "nyquist_velocity": "m s-1",
        }
        self.fix_attribute(keymap, "units")
