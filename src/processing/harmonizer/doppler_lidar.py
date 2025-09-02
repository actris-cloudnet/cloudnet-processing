import re
import shutil
from tempfile import NamedTemporaryFile
from uuid import UUID

import netCDF4
import numpy as np
from cloudnetpy.instruments import instruments

from processing.harmonizer import core
from processing.utils import MiscError


def harmonize_doppler_lidar_wind_file(
    data: dict, instrument: instruments.Instrument
) -> UUID:
    """Harmonizes Doppler lidar wind netCDF file."""
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
        wind = DopplerLidarWindNc(nc_raw, nc, data)
        valid_ind = wind.get_valid_time_indices()
        wind.copy_file(valid_ind)
        wind.add_geolocation()
        wind.height_to_asl_height()
        wind.add_date()
        wind.add_global_attributes("doppler-lidar-wind", instrument)
        uuid = wind.add_uuid()
        wind.add_history("doppler-lidar-wind")
        wind.harmonise_serial_number()
        if "azimuth_offset_deg" in data:
            wind.nc.history += "\nAzimuthal correction applied."
    if "output_path" not in data:
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


def harmonize_doppler_lidar_stare_file(
    data: dict, instrument: instruments.Instrument
) -> UUID:
    """Harmonizes Doppler lidar wind netCDF file."""
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
        stare = DopplerLidarStareNc(nc_raw, nc, data)
        valid_ind = stare.get_valid_time_indices()
        stare.copy_file(valid_ind)
        stare.clean_global_attributes()
        stare.add_geolocation()
        stare.add_date()
        stare.add_global_attributes("doppler-lidar", instrument)
        uuid = stare.add_uuid()
        stare.add_zenith_angle()
        stare.check_zenith_angle()
        stare.add_range()
        stare.clean_variable_attributes()
        stare.fix_time_units()
        stare.add_wavelength()
        for attribute in ("units", "long_name", "standard_name"):
            stare.harmonize_attribute(attribute)
        stare.fix_long_names()
        stare.add_history("doppler-lidar")
        stare.harmonise_serial_number()
    if "output_path" not in data:
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


class DopplerLidarWindNc(core.Level1Nc):
    def copy_file(self, valid_ind: list) -> None:
        keys = (
            "time",
            "height",
            "uwind",
            "vwind",
            "uwind_raw",
            "vwind_raw",
            "azimuth_offset",
        )
        self.copy_file_contents(keys, valid_ind)

    def height_to_asl_height(self) -> None:
        self.nc.variables["height"][:] += self.nc.variables["altitude"][:]
        self.nc.variables["height"].standard_name = "height_above_mean_sea_level"
        self.nc.variables["height"].long_name = "Height above mean sea level"

    def harmonise_serial_number(self) -> None:
        if "serial_number" in self.nc.ncattrs():
            self.nc.serial_number = _harmonise_doppler_lidar_serial_number(
                self.nc.serial_number
            )


class DopplerLidarStareNc(core.Level1Nc):
    def clean_global_attributes(self) -> None:
        for attr in self.nc.ncattrs():
            if attr == "filename":
                delattr(self.nc, attr)
            elif attr == "system_id":
                self.nc.serial_number = getattr(self.nc, attr)
                delattr(self.nc, attr)

    def copy_file(self, valid_ind: list) -> None:
        """Copies useful variables only."""
        keys = (
            "beta",
            "beta_raw",
            "beta_cross",
            "beta_cross_raw",
            "depolarisation",
            "depolarisation_raw",
            "v",
            "time",
            "wavelength",
            "polariser_bleed_through",
            "ray_accumulation_time",
            "pulses_per_ray",
            "elevation",
            "range",
        )
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
        """Converts halo 'range', which is actually height, to true range
        (towards LOS)."""
        self.nc.renameVariable("range", "height")
        self.copy_variable("range")
        zenith_angle = np.median(self.nc.variables["zenith_angle"][:])
        self.nc.variables["range"][:] /= np.cos(np.radians(zenith_angle))
        self.nc.variables["height"][:] += self.nc.variables["altitude"][:]

    def add_wavelength(self) -> None:
        """Converts wavelength m to nm."""
        self.nc.variables["wavelength"][:] *= 1e9

    def fix_time_units(self) -> None:
        """Fixes time units."""
        self.nc.variables["time"].units = self._get_time_units()
        self.nc.variables["time"].calendar = "standard"

    def fix_long_names(self) -> None:
        if "depolarisation_raw" in self.nc.variables:
            self.nc.variables[
                "depolarisation_raw"
            ].long_name = "Lidar volume linear depolarisation ratio"
        if "depolarisation" in self.nc.variables:
            self.nc.variables[
                "depolarisation"
            ].long_name = "Lidar volume linear depolarisation ratio"
        if "polariser_bleed_through" in self.nc.variables:
            self.nc.variables[
                "polariser_bleed_through"
            ].long_name = "Polariser bleed-through"
        if "pulses_per_ray" in self.nc.variables:
            self.nc.variables["pulses_per_ray"].long_name = "Pulses per ray"
        if "ray_accumulation_time" in self.nc.variables:
            self.nc.variables[
                "ray_accumulation_time"
            ].long_name = "Ray accumulation time"
        if "beta_cross" in self.nc.variables:
            self.nc.variables[
                "beta_cross"
            ].long_name = (
                "Attenuated backscatter coefficient for the cross-polarised signal"
            )
        if "beta_cross_raw" in self.nc.variables:
            self.nc.variables[
                "beta_cross_raw"
            ].long_name = (
                "Attenuated backscatter coefficient for the cross-polarised signal"
            )

    def harmonise_serial_number(self) -> None:
        if "serial_number" in self.nc.ncattrs():
            self.nc.serial_number = _harmonise_doppler_lidar_serial_number(
                self.nc.serial_number
            )


def _harmonise_doppler_lidar_serial_number(serial_number: str) -> str:
    if match_ := re.match(r"wls\d+s?-(\d+)", serial_number, re.IGNORECASE):
        return match_.group(1)
    elif match_ := re.match(r"wcs0*(\d+)", serial_number, re.IGNORECASE):
        return match_.group(1)
    else:
        return serial_number
