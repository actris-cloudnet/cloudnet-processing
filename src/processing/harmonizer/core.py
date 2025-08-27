import logging
import re

import cftime
import cloudnetpy.exceptions
import cloudnetpy.instruments.instruments
import cloudnetpy.metadata
import cloudnetpy.output
import cloudnetpy.utils
import netCDF4
import numpy as np

from processing.version import __version__ as cloudnet_processing_version


class Level1Nc:
    def __init__(
        self, nc_raw: netCDF4.Dataset, nc: netCDF4.Dataset, data: dict
    ) -> None:
        self.nc_raw = nc_raw
        self.nc = nc
        self.data = data

    def convert_time(self) -> None:
        """Converts time to decimal hours."""
        time = self.nc.variables["time"]
        calendar = getattr(time, "calendar", "standard")
        units = self._fix_units(time.units)
        dates = cftime.num2date(time[:], units=units, calendar=calendar)
        time_units = self._get_time_units()
        decimal_hours = cftime.date2num(dates, units=time_units, calendar="standard")
        time[:] = decimal_hours
        for attr in time.ncattrs():
            delattr(time, attr)
        time.calendar = "standard"
        time.long_name = "Time UTC"
        time.standard_name = "time"
        time.axis = "T"
        time.units = time_units

    @staticmethod
    def _fix_units(units: str) -> str:
        """Converts units to standard form."""
        patterns = (
            r"(\w+) since (\d+)/(\d+)/(\d+) (\d+:\d+:\d+)",  # seconds since M/D/YYYY HH:MM:SS
            r"(\w+) since (\d+)\.(\d+)\.(\d+), (\d+:\d+:\d+)",  # seconds since M.D.YYYY, HH:MM:SS
        )
        for pattern in patterns:
            if match := re.match(pattern, units):
                time_unit = match.group(1)
                month = int(match.group(2))
                day = int(match.group(3))
                year = int(match.group(4))
                hhmmss = match.group(5)
                return f"{time_unit} since {year:04d}-{month:02d}-{day:02d} {hhmmss}"
        return units

    def copy_file_contents(
        self,
        keys: tuple | None = None,
        time_ind: list | None = None,
        skip: tuple | None = None,
    ) -> None:
        """Copies all variables and global attributes from one file to another.
        Optionally copies only certain keys and / or uses certain time indices only.
        """
        for name, dimension in self.nc_raw.dimensions.items():
            n = (
                len(time_ind)
                if name == "time" and time_ind is not None
                else dimension.size
            )
            self.nc.createDimension(name, n)
        keys_to_process = keys if keys is not None else self.nc_raw.variables.keys()
        for key in keys_to_process:
            if skip is None or key not in skip:
                self.copy_variable(key, time_ind)
        self._copy_global_attributes()

    def copy_variable(self, key: str, time_ind: list | None = None) -> None:
        """Copies one variable from source file to target. Optionally uses certain
        time indices only.
        """
        if key not in self.nc_raw.variables.keys():
            logging.warning(f"Key {key} not found from the source file.")
            return
        variable = self.nc_raw.variables[key]
        dtype = (
            "f8" if key == "time" and "int" in str(variable.dtype) else variable.dtype
        )
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

    def add_geolocation(self) -> None:
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
    ) -> None:
        """Adds standard global attributes."""
        location = self.data["site_meta"]["name"]
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

    def add_history(self, product: str, source: str = "history") -> None:
        """Adds history attribute."""
        old_history = getattr(self.nc_raw, source, "")
        history = (
            f"{cloudnetpy.utils.get_time()} - {product} metadata harmonized by CLU using "
            f"cloudnet-processing v{cloudnet_processing_version}"
        )
        if len(old_history) > 0:
            history = f"{history}\n{old_history}"
        self.nc.history = history

    def add_date(self) -> None:
        """Adds date attributes."""
        self.nc.year, self.nc.month, self.nc.day = self.data["date"].split("-")

    def harmonize_attribute(self, attribute: str, keys: tuple | None = None) -> None:
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

    def harmonize_standard_attributes(self, key: str) -> None:
        """Harmonizes standard attributes of one variable."""
        for attribute in ("units", "long_name", "standard_name"):
            self.harmonize_attribute(attribute, (key,))

    def clean_variable_attributes(self, accepted_extra: tuple | None = None) -> None:
        """Removes obsolete variable attributes."""
        accepted = ("_FillValue", "units") + (accepted_extra or ())
        for _, item in self.nc.variables.items():
            for attr in item.ncattrs():
                if attr not in accepted:
                    delattr(item, attr)

    def clean_global_attributes(self) -> None:
        """Removes all global attributes."""
        for attr in self.nc.ncattrs():
            delattr(self.nc, attr)

    def fix_name(self, keymap: dict) -> None:
        for old_name, new_name in keymap.items():
            if old_name in self.nc.variables:
                self.nc.renameVariable(old_name, new_name)

    def fix_attribute(self, keymap: dict, attribute: str) -> None:
        """Fixes one attribute."""
        if attribute not in ("units", "long_name", "standard_name"):
            raise ValueError
        for key, value in keymap.items():
            if key in self.nc.variables:
                self.nc.variables[key].__setattr__(attribute, value)

    def get_valid_time_indices(self) -> list:
        """Finds valid time indices."""
        # Handle old Leipzig Parsivel files
        if "Meas_Time" in self.nc_raw.variables:
            time = self.nc_raw.variables["Meas_Time"]
            time_stamps = time[:]
            if len(time_stamps) < 2:
                raise cloudnetpy.exceptions.ValidTimeStampError
            raw_time_stamps = time_stamps.copy()
            time_stamps = np.array(cloudnetpy.utils.seconds2hours(time_stamps))
            max_time = 24
        else:
            supported_time_vars = ("time", "datetime")
            for time_var in supported_time_vars:
                if time_var in self.nc_raw.variables:
                    time = self.nc_raw.variables[time_var]
                    break
            else:
                raise RuntimeError(
                    f"Time variable not found from {supported_time_vars}"
                )
            time_stamps = time[:]
            if len(time_stamps) < 2:
                raise cloudnetpy.exceptions.ValidTimeStampError

            raw_time_stamps = time_stamps.copy()

            if "seconds" in time.units:
                time_stamps = np.array(cloudnetpy.utils.seconds2hours(time_stamps))

            max_time = 1440 if "minutes" in time.units else 24
        valid_ind: list[int] = []
        for ind, t in enumerate(time_stamps):
            if 0 <= t < max_time:
                if t < 0 or raw_time_stamps[ind] < 0:
                    continue
                if len(valid_ind) > 0 and t <= time_stamps[valid_ind[-1]]:
                    continue
                valid_ind.append(ind)
        if len(valid_ind) < 2:
            raise cloudnetpy.exceptions.ValidTimeStampError
        return valid_ind

    def to_ms1(self, variable: str) -> None:
        """Converts velocity to m s-1."""
        target_unit = "m s-1"
        if not hasattr(self.nc.variables[variable], "units"):
            self._set_fallback_unit(variable, target_unit)
            return
        units = self.nc.variables[variable].units.lower()
        match units:
            case "m/s" | "m s-1" | "m / s":
                factor = None
            case "mm/h" | "mm/hour" | "mm h-1" | "mm / h" | "mm / hour":
                factor = 1e-3 / 3600
            case "mm/min" | "mm min-1" | "mm / min":
                factor = 1e-3 / 60
            case "mm/s" | "mm s-1" | "mm / s":
                factor = 1e-3
            case _:
                raise ValueError(
                    f"Variable '{variable}' has unsupported units: {units}"
                )
        if factor:
            self.nc.variables[variable][:] *= factor
            logging.info(f"Converting {variable} from {units} to {target_unit}.")
        self.nc.variables[variable].units = target_unit

    def to_m(self, variable: str) -> None:
        """Converts length to m."""
        target_unit = "m"
        if not hasattr(self.nc.variables[variable], "units"):
            self._set_fallback_unit(variable, target_unit)
            return
        units = self.nc.variables[variable].units.lower()
        match units:
            case "m":
                factor = None
            case "mm":
                factor = 1e-3
            case _:
                raise ValueError(
                    f"Variable '{variable}' has unsupported units: {units}"
                )
        if factor:
            self.nc.variables[variable][:] *= factor
            logging.info(f"Converting {variable} from {units} to {target_unit}.")
        self.nc.variables[variable].units = target_unit

    def to_ratio(self, variable: str) -> None:
        """Converts percent to ratio."""
        target_unit = "1"
        if not hasattr(self.nc.variables[variable], "units"):
            self._set_fallback_unit(variable, target_unit)
            return
        units = self.nc.variables[variable].units.lower()
        match units:
            case "1" | "":
                factor = None
            case "%" | "percent":
                factor = 1e-2
            case _:
                raise ValueError(
                    f"Variable '{variable}' has unsupported units: {units}"
                )
        if factor:
            self.nc.variables[variable][:] *= factor
            logging.info(f"Converting {variable} from {units} to {target_unit}.")
        self.nc.variables[variable].units = target_unit

    def to_pa(self, variable: str) -> None:
        """Converts pressure to Pa."""
        target_unit = "Pa"
        if not hasattr(self.nc.variables[variable], "units"):
            self._set_fallback_unit(variable, target_unit)
            return
        units = self.nc.variables[variable].units.lower()
        match units:
            case "pa":
                factor = None
            case "hpa":
                factor = 100.0
            case _:
                raise ValueError(
                    f"Variable '{variable}' has unsupported units: {units}"
                )
        if factor:
            self.nc.variables[variable][:] *= factor
            logging.info(f"Converting {variable} from {units} to {target_unit}.")
        self.nc.variables[variable].units = target_unit

    def to_degree(self, variable: str) -> None:
        """Converts direction to degree."""
        target_unit = "degree"
        if not hasattr(self.nc.variables[variable], "units"):
            self._set_fallback_unit(variable, target_unit)
            return
        units = self.nc.variables[variable].units.lower()
        match units:
            case "degrees" | "degree":
                factor = None
            case _:
                raise ValueError(
                    f"Variable '{variable}' has unsupported units: {units}"
                )
        if factor:
            self.nc.variables[variable][:] *= factor
            logging.info(f"Converting {variable} from {units} to {target_unit}.")
        self.nc.variables[variable].units = target_unit

    def to_k(self, variable: str) -> None:
        target_unit = "K"
        if not hasattr(self.nc.variables[variable], "units"):
            self._set_fallback_unit(variable, target_unit)
            return
        units = self.nc.variables[variable].units.lower()
        match units:
            case "k":
                pass
            case (
                "c"
                | "celsius"
                | "degc"
                | "°c"
                | "deg c"
                | "degree celsius"
                | "degrees celsius"
            ):
                self.nc.variables[variable][:] += 273.15
                logging.info(f"Converting {variable} from {units} to {target_unit}.")
            case _:
                raise ValueError(
                    f"Variable '{variable}' has unsupported units: {units}"
                )
        self.nc.variables[variable].units = target_unit

    def _set_fallback_unit(self, variable: str, fallback: str) -> None:
        logging.warning(f"No units attribute in '{variable}'! Assuming '{fallback}'.")
        self.nc.variables[variable].units = fallback

    def _copy_global_attributes(self) -> None:
        for name in self.nc_raw.ncattrs():
            setattr(self.nc, name, self.nc_raw.getncattr(name))

    def _get_time_units(self) -> str:
        return f"hours since {self.data['date']} 00:00:00 +00:00"

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
    def _copy_variable_attributes(
        source: netCDF4.Variable, target: netCDF4.Variable
    ) -> None:
        attr = {k: source.getncattr(k) for k in source.ncattrs() if k != "_FillValue"}
        target.setncatts(attr)
