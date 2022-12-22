import datetime
import logging
import shutil
from tempfile import NamedTemporaryFile

import cloudnetpy.utils
import netCDF4
import numpy as np
from cloudnetpy import output
from cloudnetpy.exceptions import ValidTimeStampError
from cloudnetpy.instruments import instruments
from cloudnetpy.metadata import COMMON_ATTRIBUTES
from cloudnetpy.utils import get_time, get_uuid, seconds2date

from data_processing import utils
from data_processing.utils import MiscError


def fix_legacy_file(legacy_file_full_path: str, target_full_path: str, data: dict) -> str:
    """Fixes legacy netCDF file."""
    with (
        netCDF4.Dataset(legacy_file_full_path, "r") as nc_legacy,
        netCDF4.Dataset(target_full_path, "w", format="NETCDF4_CLASSIC") as nc,
    ):
        legacy = Level1Nc(nc_legacy, nc, data)
        legacy.copy_file_contents()
        uuid = legacy.add_uuid()
        legacy.add_history("")

        if legacy.nc.location == "polarstern":
            legacy.nc.cloudnetpy_version = f"Custom CloudnetPy ({legacy.nc.cloudnetpy_version})"
            bad_att_attr_names = ["dependencies", "comment"]
            if legacy.nc.cloudnet_file_type == "categorize":
                bad_att_attr_names.append("source_file_uuids")
            for attr_name in bad_att_attr_names:
                try:
                    delattr(legacy.nc, attr_name)
                except AttributeError:
                    pass
    return uuid


def harmonize_model_file(data: dict) -> str:
    """Harmonizes model netCDF file."""
    temp_file = NamedTemporaryFile()
    with (
        netCDF4.Dataset(data["full_path"], "r") as nc_raw,
        netCDF4.Dataset(temp_file.name, "w", format="NETCDF4_CLASSIC") as nc,
    ):
        model = ModelNc(nc_raw, nc, data)
        model.copy_file_contents()
        model.harmonize_attribute("units", ("latitude", "longitude", "altitude"))
        uuid = model.add_uuid()
        model.add_global_model_attributes()
        model.check_time_dimension()
        model.add_date()
        model.add_history("model")
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


def harmonize_hatpro_file(data: dict) -> str:
    """Harmonizes calibrated HATPRO netCDF file."""
    temp_file = NamedTemporaryFile()
    with (
        netCDF4.Dataset(data["full_path"], "r") as nc_raw,
        netCDF4.Dataset(temp_file.name, "w", format="NETCDF4_CLASSIC") as nc,
    ):
        hatpro = HatproNc(nc_raw, nc, data)
        hatpro.copy_file()
        hatpro.add_lwp()
        hatpro.check_lwp_data()
        hatpro.sort_time()
        hatpro.convert_time()
        hatpro.check_time_reference()
        hatpro.add_geolocation()
        hatpro.clean_global_attributes()
        uuid = hatpro.add_uuid()
        hatpro.add_date()
        hatpro.add_global_attributes("mwr", instruments.HATPRO)
        hatpro.add_history("mwr")
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


def harmonize_halo_file(data: dict) -> str:
    """Harmonizes calibrated HALO Doppler lidar netCDF file."""
    temp_file = NamedTemporaryFile()
    with (
        netCDF4.Dataset(data["full_path"], "r") as nc_raw,
        netCDF4.Dataset(temp_file.name, "w", format="NETCDF4_CLASSIC") as nc,
    ):
        halo = HaloNc(nc_raw, nc, data)
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
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


class Level1Nc:
    def __init__(self, nc_raw: netCDF4.Dataset, nc: netCDF4.Dataset, data: dict):
        self.nc_raw = nc_raw
        self.nc = nc
        self.data = data

    def copy_file_contents(self, keys: tuple | None = None, time_ind: list | None = None):
        """Copies all variables and global attributes from one file to another.
        Optionally copies only certain keys and / or uses certain time indices only.
        """
        for key, dimension in self.nc_raw.dimensions.items():
            if key == "time" and time_ind is not None:
                self.nc.createDimension(key, len(time_ind))
            else:
                self.nc.createDimension(key, dimension.size)
        keys = keys if keys is not None else self.nc_raw.variables.keys()
        for key in keys:
            self.copy_variable(key, time_ind)
        self._copy_global_attributes()

    def copy_variable(self, key: str, time_ind: list | None = None):
        """Copies one variable from source file to target. Optionally uses certain
        time indices only.
        """
        if key not in self.nc_raw.variables.keys():
            logging.warning(f"Key {key} not found from the source file.")
            return
        variable = self.nc_raw.variables[key]
        dtype = variable.dtype
        var_out = self.nc.createVariable(
            key,
            dtype,
            variable.dimensions,
            zlib=True,
            fill_value=getattr(variable, "_FillValue", None),
        )
        self._copy_variable_attributes(variable, var_out)
        screened_data = self._screen_data(variable, time_ind)
        var_out[:] = screened_data

    def add_geolocation(self):
        """Adds standard geolocation information."""
        for key in ("altitude", "latitude", "longitude"):
            if key not in self.nc.variables.keys():
                var = self.nc.createVariable(key, "f4")
            else:
                var = self.nc.variables[key]
            var[:] = self.data["site_meta"][key]
            self.harmonize_standard_attributes(key)

    def add_global_attributes(self, cloudnet_file_type: str, instrument: instruments.Instrument):
        """Adds standard global attributes."""
        location = utils.read_site_info(self.data["site_name"])["name"]
        self.nc.Conventions = "CF-1.8"
        self.nc.cloudnet_file_type = cloudnet_file_type
        self.nc.source = output.get_l1b_source(instrument)
        self.nc.location = location
        self.nc.title = output.get_l1b_title(instrument, location)
        self.nc.references = output.get_references()

    def add_uuid(self) -> str:
        """Adds UUID."""
        uuid = self.data["uuid"] or get_uuid()
        self.nc.file_uuid = uuid
        return uuid

    def add_history(self, product: str):
        """Adds history attribute."""
        version = utils.get_data_processing_version()
        old_history = getattr(self.nc_raw, "history", "")
        history = (
            f"{get_time()} - {product} metadata harmonized by CLU using "
            f"cloudnet-processing v{version}"
        )
        if len(old_history) > 0:
            history = f"{history}\n{old_history}"
        self.nc.history = history

    def add_date(self):
        """Adds date attributes."""
        self.nc.year, self.nc.month, self.nc.day = self.data["date"].split("-")

    def harmonize_attribute(self, attribute: str, keys: tuple | None = None):
        """Harmonizes variable attributes."""
        keys = keys if keys is not None else self.nc.variables.keys()
        for key in keys:
            value = getattr(COMMON_ATTRIBUTES.get(key), attribute, None)
            if value is not None and key in self.nc.variables:
                setattr(self.nc.variables[key], attribute, value)
            else:
                logging.debug(f'Can"t find {attribute} for {key}')

    def harmonize_standard_attributes(self, key: str):
        """Harmonizes standard attributes of one variable."""
        for attribute in ("units", "long_name", "standard_name"):
            self.harmonize_attribute(attribute, (key,))

    def clean_variable_attributes(self):
        """Removes obsolete variable attributes."""
        accepted = ("_FillValue", "units")
        for _, item in self.nc.variables.items():
            for attr in item.ncattrs():
                if attr not in accepted:
                    delattr(item, attr)

    def clean_global_attributes(self):
        """Removes all global attributes."""
        for attr in self.nc.ncattrs():
            delattr(self.nc, attr)

    def _copy_global_attributes(self):
        for name in self.nc_raw.ncattrs():
            setattr(self.nc, name, self.nc_raw.getncattr(name))

    def _get_time_units(self) -> str:
        return f'hours since {self.data["date"]} 00:00:00 +00:00'

    @staticmethod
    def _screen_data(variable: netCDF4.Variable, time_ind: list | None = None) -> np.ndarray:
        if variable.ndim > 0 and time_ind is not None and variable.dimensions[0] == "time":
            if variable.ndim == 1:
                return variable[time_ind]
            if variable.ndim == 2:
                return variable[time_ind, :]
        return variable[:]

    @staticmethod
    def _copy_variable_attributes(source, target):
        attr = {k: source.getncattr(k) for k in source.ncattrs() if k != "_FillValue"}
        target.setncatts(attr)


class ModelNc(Level1Nc):
    def check_time_dimension(self):
        """Checks time dimension."""
        n_steps = len(self.nc.dimensions["time"])
        n_steps_expected = 25
        n_steps_expected_gdas1 = 9
        if self.data["model"] == "gdas1" and n_steps == n_steps_expected_gdas1:
            return
        if self.data["model"] != "gdas1" and n_steps == n_steps_expected:
            return
        raise MiscError("Incomplete model file.")

    def add_date(self):
        """Adds data in correct format."""
        date_string = self.nc.variables["time"].units
        date = date_string.split()[2]
        self.nc.year, self.nc.month, self.nc.day = date.split("-")

    def add_global_model_attributes(self):
        """Adds required global attributes."""
        self.nc.cloudnet_file_type = "model"
        self.nc.Conventions = "CF-1.8"


class HaloNc(Level1Nc):
    def copy_file(self, valid_ind: list):
        """Copies useful variables only."""
        keys = ("beta", "beta_raw", "time", "wavelength", "elevation", "range")
        self.copy_file_contents(keys, valid_ind)

    def add_zenith_angle(self):
        """Converts elevation to zenith angle."""
        self.nc.renameVariable("elevation", "zenith_angle")
        self.nc.variables["zenith_angle"][:] = 90 - self.nc.variables["zenith_angle"][:]

    def check_zenith_angle(self):
        """Checks zenith angle value."""
        threshold = 15
        if (zenith_angle := self.nc.variables["zenith_angle"][:]) > threshold:
            raise ValueError(f"Invalid zenith angle {zenith_angle}")

    def add_range(self):
        """Converts halo 'range', which is actually height, to true range (towards LOS)."""
        self.nc.renameVariable("range", "height")
        self.copy_variable("range")
        zenith_angle = np.median(self.nc.variables["zenith_angle"][:])
        self.nc.variables["range"][:] /= np.cos(np.radians(zenith_angle))

    def add_wavelength(self):
        """Converts wavelength m to nm."""
        self.nc.variables["wavelength"][:] *= 1e9

    def fix_time_units(self):
        """Fixes time units."""
        self.nc.variables["time"].units = self._get_time_units()

    def get_valid_time_indices(self) -> list:
        """Finds valid time indices."""
        time_stamps = self.nc_raw.variables["time"][:]
        valid_ind: list[int] = []
        for ind, t in enumerate(time_stamps):
            if 0 < t < 24:
                if len(valid_ind) > 1 and t <= time_stamps[valid_ind[-1]]:
                    continue
                valid_ind.append(ind)
        if not valid_ind:
            raise ValidTimeStampError
        return valid_ind


class HatproNc(Level1Nc):

    bad_lwp_keys = ("LWP", "LWP_data", "clwvi", "atmosphere_liquid_water_content")

    def copy_file(self, all_keys: bool = False):
        """Copies essential fields only."""
        valid_ind = self._get_valid_timestamps()
        if all_keys is True:
            possible_keys = None
        else:
            possible_keys = ("lwp", "time") + self.bad_lwp_keys
        self._copy_hatpro_file_contents(valid_ind, possible_keys)

    def add_lwp(self):
        """Converts lwp and fixes its attributes."""
        key = "lwp"
        for invalid_name in self.bad_lwp_keys:
            if invalid_name in self.nc.variables:
                self.nc.renameVariable(invalid_name, key)
        assert key in self.nc.variables
        lwp = self.nc.variables[key]
        if "kg" in lwp.units:
            lwp[:] *= 1000
        self.harmonize_standard_attributes(key)
        attributes_to_be_removed = ("comment", "missing_value")
        for attr in attributes_to_be_removed:
            if hasattr(lwp, attr):
                delattr(lwp, attr)

    def check_lwp_data(self):
        """Sanity checks LWP data."""
        threshold_kg = 10
        lwp = self.nc.variables["lwp"][:]
        positive_lwp_values = lwp[lwp > 0] / 1000
        if (median_value := np.median(positive_lwp_values)) > threshold_kg:
            raise MiscError(f"Invalid LWP data, median value: {np.round(median_value, 2)} kg")

    def sort_time(self):
        """Sorts time array."""
        time = self.nc.variables["time"][:]
        array = self.nc.variables["lwp"][:]
        ind = time.argsort()
        self.nc.variables["time"][:] = time[ind]
        self.nc.variables["lwp"][:] = array[ind]

    def convert_time(self):
        """Converts time to decimal hour."""
        time = self.nc.variables["time"]
        if max(time[:]) > 24:
            fraction_hour = cloudnetpy.utils.seconds2hours(time[:])
            time[:] = fraction_hour
        time.long_name = "Time UTC"
        time.units = self._get_time_units()
        for key in ("comment", "bounds"):
            if hasattr(time, key):
                delattr(time, key)
        time.standard_name = "time"
        time.axis = "T"
        time.calendar = "standard"

    def check_time_reference(self):
        """Checks the reference time zone."""
        key = "time_reference"
        if key in self.nc_raw.variables:
            if self.nc_raw.variables[key][:] != 1:  # not UTC
                raise ValueError

    def _get_valid_timestamps(self) -> list:
        time_stamps = self.nc_raw.variables["time"][:]
        epoch = _get_epoch(self.nc_raw.variables["time"].units)
        expected_date = self.data["date"].split("-")
        valid_ind = []
        for ind, t in enumerate(time_stamps):
            if (0 < t < 24 and epoch == expected_date) or (
                seconds2date(t, epoch)[:3] == expected_date
            ):
                valid_ind.append(ind)
        if not valid_ind:
            raise ValidTimeStampError
        _, ind = np.unique(time_stamps[valid_ind], return_index=True)
        return list(np.array(valid_ind)[ind])

    def _copy_hatpro_file_contents(self, time_ind: list, keys: tuple | None = None):
        self.nc.createDimension("time", len(time_ind))
        for name, variable in self.nc_raw.variables.items():
            if keys is not None and name not in keys:
                continue
            if name == "time" and "int" in str(variable.dtype):
                dtype = "f8"
            else:
                dtype = variable.dtype
            var_out = self.nc.createVariable(
                name,
                dtype,
                variable.dimensions,
                zlib=True,
                fill_value=getattr(variable, "_FillValue", None),
            )
            self._copy_variable_attributes(variable, var_out)
            var_out[:] = variable[time_ind] if "time" in variable.dimensions else variable[:]
        self._copy_global_attributes()


def _get_epoch(units: str) -> tuple:
    fallback = (2001, 1, 1)
    try:
        date = units.split()[2]
    except IndexError:
        return fallback
    date = date.replace(",", "")
    try:
        date_components = [int(x) for x in date.split("-")]
    except ValueError:
        try:
            date_components = [int(x) for x in date.split(".")]
        except ValueError:
            return fallback
    year, month, day = date_components
    current_year = datetime.datetime.today().year
    if (1900 < year <= current_year) and (0 < month < 13) and (0 < day < 32):
        return tuple(date_components)
    return fallback
