import shutil
from tempfile import NamedTemporaryFile

import netCDF4
import numpy as np
from cloudnetpy.instruments import instruments

from processing.harmonizer import core
from processing.utils import MiscError


def harmonize_halo_calibrated_file(data: dict) -> str:
    """Harmonizes calibrated HALO Doppler lidar netCDF file."""
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
        halo = HaloNcCalibrated(nc_raw, nc, data)
        valid_ind = halo.get_valid_time_indices()
        halo.copy_file(valid_ind)
        halo.clean_global_attributes()
        halo.add_geolocation()
        halo.add_date()
        halo.add_global_attributes("lidar", instruments.HALO)
        uuid = halo.add_uuid()
        halo.add_zenith_angle()
        halo.check_zenith_angle()
        halo.add_range()
        halo.add_wavelength()
        halo.clean_variable_attributes()
        halo.fix_time_units()
        for attribute in ("units", "long_name", "standard_name"):
            halo.harmonize_attribute(attribute)
        halo.add_history("lidar")
    if "output_path" not in data:
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


class HaloNcCalibrated(core.Level1Nc):
    def copy_file(self, valid_ind: list) -> None:
        """Copies useful variables only."""
        keys = ("beta", "beta_raw", "time", "wavelength", "elevation", "range")
        self.copy_file_contents(keys, valid_ind)

    def add_zenith_angle(self) -> None:
        """Converts elevation to zenith angle."""
        self.nc.renameVariable("elevation", "zenith_angle")
        self.nc.variables["zenith_angle"][:] = 90 - self.nc.variables["zenith_angle"][:]

    def check_zenith_angle(self) -> None:
        """Checks zenith angle value."""
        threshold = 15
        if (
            zenith_angle := np.median(self.nc.variables["zenith_angle"][:])
        ) > threshold:
            raise MiscError(f"Invalid zenith angle {zenith_angle}")

    def add_range(self) -> None:
        """Converts halo 'range', which is actually height, to true range (towards LOS)."""
        self.nc.renameVariable("range", "height")
        self.copy_variable("range")
        zenith_angle = np.median(self.nc.variables["zenith_angle"][:])
        self.nc.variables["range"][:] /= np.cos(np.radians(zenith_angle))

    def add_wavelength(self) -> None:
        """Converts wavelength m to nm."""
        self.nc.variables["wavelength"][:] *= 1e9

    def fix_time_units(self) -> None:
        """Fixes time units."""
        self.nc.variables["time"].units = self._get_time_units()
        self.nc.variables["time"].calendar = "standard"
