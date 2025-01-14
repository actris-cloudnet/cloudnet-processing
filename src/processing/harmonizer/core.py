import logging

import cloudnetpy.exceptions
import cloudnetpy.instruments.instruments
import cloudnetpy.metadata
import cloudnetpy.output
import cloudnetpy.utils
import netCDF4
import numpy as np

import processing.utils


class Level1Nc:
    def __init__(self, nc_raw: netCDF4.Dataset, nc: netCDF4.Dataset, data: dict):
        self.nc_raw = nc_raw
        self.nc = nc
        self.data = data

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

    def copy_file_contents(
        self,
        keys: tuple | None = None,
        time_ind: list | None = None,
        skip: tuple | None = None,
    ):
        """Copies all variables and global attributes from one file to another.
        Optionally copies only certain keys and / or uses certain time indices only.
        """
        for key, dimension in self.nc_raw.dimensions.items():
            if key == "time" and time_ind is not None:
                self.nc.createDimension(key, len(time_ind))
            else:
                self.nc.createDimension(key, dimension.size)
        keys_to_process = keys if keys is not None else self.nc_raw.variables.keys()
        for key in keys_to_process:
            if skip is None or key not in skip:
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

        if key == "time" and "int64" in str(dtype):
            dtype = "f8"

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

    def add_global_attributes(
        self,
        cloudnet_file_type: str,
        instrument: cloudnetpy.instruments.instruments.Instrument,
    ):
        """Adds standard global attributes."""
        location = processing.utils.read_site_info(self.data["site_name"])["name"]
        self.nc.Conventions = "CF-1.8"
        self.nc.cloudnet_file_type = cloudnet_file_type
        self.nc.source = cloudnetpy.output.get_l1b_source(instrument)
        self.nc.location = location
        self.nc.title = cloudnetpy.output.get_l1b_title(instrument, location)
        self.nc.references = cloudnetpy.output.get_references()

    def add_uuid(self) -> str:
        """Adds UUID."""
        uuid = self.data["uuid"] or cloudnetpy.utils.get_uuid()
        self.nc.file_uuid = uuid
        return uuid

    def add_history(self, product: str, source: str = "history"):
        """Adds history attribute."""
        version = processing.utils.get_data_processing_version()
        old_history = getattr(self.nc_raw, source, "")
        history = (
            f"{cloudnetpy.utils.get_time()} - {product} metadata harmonized by CLU using "
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
        keys_to_process = keys if keys is not None else self.nc.variables.keys()
        for key in keys_to_process:
            value = getattr(
                cloudnetpy.metadata.COMMON_ATTRIBUTES.get(key), attribute, None
            )
            if value is not None and key in self.nc.variables:
                setattr(self.nc.variables[key], attribute, value)
            else:
                logging.debug(f"Can't find {attribute} for {key}")

    def harmonize_standard_attributes(self, key: str):
        """Harmonizes standard attributes of one variable."""
        for attribute in ("units", "long_name", "standard_name"):
            self.harmonize_attribute(attribute, (key,))

    def clean_variable_attributes(self, accepted_extra: tuple | None = None):
        """Removes obsolete variable attributes."""
        accepted = ("_FillValue", "units") + (accepted_extra or ())
        for _, item in self.nc.variables.items():
            for attr in item.ncattrs():
                if attr not in accepted:
                    delattr(item, attr)

    def clean_global_attributes(self):
        """Removes all global attributes."""
        for attr in self.nc.ncattrs():
            delattr(self.nc, attr)

    def fix_name(self, keymap: dict):
        for old_name, new_name in keymap.items():
            if old_name in self.nc.variables:
                self.nc.renameVariable(old_name, new_name)

    def fix_attribute(self, keymap: dict, attribute: str):
        """Fixes one attribute."""
        if attribute not in ("units", "long_name", "standard_name"):
            raise ValueError
        for key, value in keymap.items():
            if key in self.nc.variables:
                self.nc.variables[key].__setattr__(attribute, value)

    def get_valid_time_indices(self) -> list:
        """Finds valid time indices."""
        time = self.nc_raw.variables["time"]
        time_stamps = time[:]

        if "seconds since" in time.units:
            time_stamps = np.array(cloudnetpy.utils.seconds2hours(time_stamps))
        max_time = 1440 if "minutes" in time.units else 24
        valid_ind: list[int] = []
        for ind, t in enumerate(time_stamps):
            if 0 <= t < max_time:
                if t < 0:
                    continue
                if len(valid_ind) > 1 and t <= time_stamps[valid_ind[-1]]:
                    continue
                valid_ind.append(ind)
        if not valid_ind:
            raise cloudnetpy.exceptions.ValidTimeStampError
        return valid_ind

    def _copy_global_attributes(self):
        for name in self.nc_raw.ncattrs():
            setattr(self.nc, name, self.nc_raw.getncattr(name))

    def _get_time_units(self) -> str:
        return f'hours since {self.data["date"]} 00:00:00 +00:00'

    @staticmethod
    def _screen_data(
        variable: netCDF4.Variable, time_ind: list | None = None
    ) -> np.ndarray:
        if (
            variable.ndim > 0
            and time_ind is not None
            and variable.dimensions[0] in ("time", "dim")
        ):
            if variable.ndim == 1:
                return variable[time_ind]
            if variable.ndim == 2:
                return variable[time_ind, :]
            if variable.ndim == 3:
                return variable[time_ind, :, :]
        return variable[:]

    @staticmethod
    def _copy_variable_attributes(source, target):
        attr = {k: source.getncattr(k) for k in source.ncattrs() if k != "_FillValue"}
        target.setncatts(attr)


def to_ms1(variable: str, data: np.ndarray, units: str) -> np.ndarray:
    if units.lower() in ("mm h", "mm h-1", "mm/h", "mm / h"):
        logging.info(f'Converting {variable} from "{units}" to "m s-1".')
        data /= 1000 * 3600
    elif units.lower() not in ("m s-1", "m/s", "m / s"):
        raise ValueError(f"Unsupported unit {units} in variable {variable}")
    return data
