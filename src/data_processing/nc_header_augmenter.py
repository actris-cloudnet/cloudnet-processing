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
from cloudnetpy.instruments.disdrometer import ATTRIBUTES
from cloudnetpy.metadata import COMMON_ATTRIBUTES
from cloudnetpy.utils import get_time, get_uuid, seconds2date

from data_processing import utils
from data_processing.utils import MiscError


def fix_legacy_file(
    legacy_file_full_path: str, target_full_path: str, data: dict
) -> str:
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
            legacy.nc.cloudnetpy_version = (
                f"Custom CloudnetPy ({legacy.nc.cloudnetpy_version})"
            )

            if legacy.nc_raw.cloudnet_file_type == "lidar":
                legacy.nc.instrument_pid = (
                    "https://hdl.handle.net/21.12132/3.31c4f71cf1a74e03"
                )

            if hasattr(legacy.nc, "source_file_uuids"):
                valid_uuids = []
                for source_uuid in legacy.nc.source_file_uuids.split(", "):
                    res = utils.get_from_data_portal_api(f"api/files/{source_uuid}")
                    if isinstance(res, dict) and res.get("status") != 404:
                        valid_uuids.append(source_uuid)
                legacy.nc.source_file_uuids = ", ".join(valid_uuids)

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
    """Harmonizes HALO Doppler lidar netCDF file."""
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
        halo.add_global_attributes("doppler-lidar", instruments.HALO)
        uuid = halo.add_uuid()
        halo.add_zenith_angle()
        halo.check_zenith_angle()
        halo.add_range()
        halo.clean_variable_attributes()
        halo.fix_time_units()
        halo.add_wavelength()
        for attribute in ("units", "long_name", "standard_name"):
            halo.harmonize_attribute(attribute)
        halo.add_history("lidar")
    shutil.copy(temp_file.name, data["full_path"])
    return uuid


def harmonize_halo_calibrated_file(data: dict) -> str:
    """Harmonizes calibrated HALO Doppler lidar netCDF file."""
    temp_file = NamedTemporaryFile()
    with (
        netCDF4.Dataset(data["full_path"], "r") as nc_raw,
        netCDF4.Dataset(temp_file.name, "w", format="NETCDF4_CLASSIC") as nc,
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
    shutil.copy(temp_file.name, data["full_path"])
    return uuid


def harmonize_parsivel_file(data: dict) -> str:
    temp_file = NamedTemporaryFile()
    with netCDF4.Dataset(data["full_path"], "r") as nc_raw, netCDF4.Dataset(
        temp_file.name, "w", format="NETCDF4_CLASSIC"
    ) as nc:
        parsivel = ParsivelNc(nc_raw, nc, data)
        valid_ind = parsivel.get_valid_time_indices()
        parsivel.copy_file_contents(
            time_ind=valid_ind,
            skip=(
                "lat",
                "lon",
                "zsl",
                "time_bnds",
                "timestamp",
                "datetime",
                "code_4678",
                "code_NWS",
            ),
        )
        parsivel.fix_variable_names()
        parsivel.convert_time()
        parsivel.add_date()
        parsivel.add_geolocation()
        uuid = parsivel.add_uuid()
        parsivel.add_global_attributes("disdrometer", instruments.PARSIVEL2)
        parsivel.add_history("disdrometer", source="History")
        parsivel.fix_global_attributes()
        parsivel.clean_global_attributes()
        parsivel.fix_long_names()
        parsivel.fix_units()
        parsivel.fix_standard_names()
        parsivel.fix_comments()
    shutil.copy(temp_file.name, data["full_path"])
    return uuid


def harmonize_ws_file(data: dict) -> str:
    """Harmonizes weather station netCDF file."""
    temp_file = NamedTemporaryFile()
    with (
        netCDF4.Dataset(data["full_path"], "r") as nc_raw,
        netCDF4.Dataset(temp_file.name, "w", format="NETCDF4_CLASSIC") as nc,
    ):
        ws = Ws(nc_raw, nc, data)
        valid_ind = ws.get_valid_time_indices()
        ws.copy_ws_file_contents(time_ind=valid_ind)
        ws.fix_variable_names()
        ws.clean_global_attributes()
        ws.add_global_attributes("weather-station", instruments.GENERIC_WEATHER_STATION)
        ws.add_geolocation()
        ws.add_date()
        uuid = ws.add_uuid()
        ws.clean_variable_attributes(
            accepted_extra=("flag_values", "flag_meanings", "ancillary_variables")
        )
        for attribute in ("units", "long_name", "standard_name"):
            ws.harmonize_attribute(attribute)
        ws.add_history("weather-station")
        ws.nc.source = "Weather station"
        ws.fix_time()
        ws.fix_rainfall_variables()
        ws.fix_standard_names()
        ws.fix_long_names()
        ws.fix_flag_attributes()
        ws.fix_ancillary_variable_names()
    shutil.copy(temp_file.name, data["full_path"])
    return uuid


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
        keys = keys if keys is not None else self.nc_raw.variables.keys()
        for key in keys:
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
        self, cloudnet_file_type: str, instrument: instruments.Instrument
    ):
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

    def add_history(self, product: str, source: str = "history"):
        """Adds history attribute."""
        version = utils.get_data_processing_version()
        old_history = getattr(self.nc_raw, source, "")
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
            if 0 < t < max_time:
                if len(valid_ind) > 1 and t <= time_stamps[valid_ind[-1]]:
                    continue
                valid_ind.append(ind)
        if not valid_ind:
            raise ValidTimeStampError
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
            and variable.dimensions[0] == "time"
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
        """Adds date in correct format."""
        date_string = self.nc.variables["time"].units
        date = date_string.split()[2]
        self.nc.year, self.nc.month, self.nc.day = date.split("-")

    def add_global_model_attributes(self):
        """Adds required global attributes."""
        self.nc.cloudnet_file_type = "model"
        self.nc.Conventions = "CF-1.8"


class HaloNc(Level1Nc):
    def clean_global_attributes(self):
        for attr in self.nc.ncattrs():
            if attr == "filename":
                delattr(self.nc, attr)
            elif attr == "system_id":
                self.nc.serial_number = getattr(self.nc, attr)
                delattr(self.nc, attr)

    def copy_file(self, valid_ind: list):
        """Copies useful variables only."""
        keys = (
            "beta",
            "beta_raw",
            "v",
            "time",
            "wavelength",
            "elevation",
            "range",
        )
        self.copy_file_contents(keys, valid_ind)

    def add_zenith_angle(self):
        """Converts elevation to zenith angle."""
        self.nc.renameVariable("elevation", "zenith_angle")
        self.nc.variables["zenith_angle"][:] = 90 - self.nc.variables["zenith_angle"][:]

    def check_zenith_angle(self):
        """Checks zenith angle value."""
        threshold = 15
        if (
            zenith_angle := np.median(self.nc.variables["zenith_angle"][:])
        ) > threshold:
            raise MiscError(f"Invalid zenith angle {zenith_angle}")

    def add_range(self):
        """Converts halo 'range', which is actually height, to true range
        (towards LOS)."""
        self.nc.renameVariable("range", "height")
        self.copy_variable("range")
        zenith_angle = np.median(self.nc.variables["zenith_angle"][:])
        self.nc.variables["range"][:] /= np.cos(np.radians(zenith_angle))
        self.nc.variables["height"][:] += self.nc.variables["altitude"][:]

    def add_wavelength(self):
        """Converts wavelength m to nm."""
        self.nc.variables["wavelength"][:] *= 1e9

    def fix_time_units(self):
        """Fixes time units."""
        self.nc.variables["time"].units = self._get_time_units()
        self.nc.variables["time"].calendar = "standard"


class HaloNcCalibrated(Level1Nc):
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
        if (
            zenith_angle := np.median(self.nc.variables["zenith_angle"][:])
        ) > threshold:
            raise MiscError(f"Invalid zenith angle {zenith_angle}")

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
        self.nc.variables["time"].calendar = "standard"


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
        try:
            lwp = self.nc.variables[key]
        except KeyError:
            raise MiscError(f"Missing mandatory variable {key} - abort processing")
        if "kg" not in lwp.units:
            lwp[:] /= 1000
        self.harmonize_standard_attributes(key)
        attributes_to_be_removed = ("comment", "missing_value")
        for attr in attributes_to_be_removed:
            if hasattr(lwp, attr):
                delattr(lwp, attr)

    def check_lwp_data(self):
        """Sanity checks LWP data."""
        threshold_kg = 10
        lwp = self.nc.variables["lwp"][:]
        positive_lwp_values = lwp[lwp > 0]
        if (median_value := np.median(positive_lwp_values)) > threshold_kg:
            raise MiscError(
                f"Invalid LWP data, median value: {np.round(median_value, 2)} kg"
            )

    def sort_time(self):
        """Sorts time array."""
        time = self.nc.variables["time"][:]
        array = self.nc.variables["lwp"][:]
        ind = time.argsort()
        self.nc.variables["time"][:] = time[ind]
        self.nc.variables["lwp"][:] = array[ind]

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
            var_out[:] = (
                variable[time_ind] if "time" in variable.dimensions else variable[:]
            )
        self._copy_global_attributes()


def _get_epoch(units: str) -> tuple[int, int, int]:
    fallback = 2001, 1, 1
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
        return year, month, day
    return fallback


class ParsivelNc(Level1Nc):
    def fix_variable_names(self):
        keymap = {
            "V_sensor": "V_power_supply",
            "E_kin": "kinetic_energy",
            "Synop_WaWa": "synop_WaWa",
            "rain_intensity": "rainfall_rate",
            "MOR": "visibility",
            "reflectivity": "radar_reflectivity",
            "fieldN": "number_concentration",
            "fieldV": "fall_velocity",
            "amplitude": "sig_laser",
            "time_interval": "interval",
            "snowfall_intensity": "snowfall_rate",
            "code_4680": "synop_WaWa",
            "code_4677": "synop_WW",
            "velocity_center_classes": "velocity",
            "diameter_center_classes": "diameter",
        }
        self.fix_name(keymap)

    def fix_global_attributes(self):
        for attr in ("Sensor_ID", "sensor_serial_number"):
            if hasattr(self.nc, attr):
                self.nc.serial_number = getattr(self.nc, attr)

    def clean_global_attributes(self):
        keep_case_sensitive = {"history", "source", "title"}
        ignore_case_insensitive = {
            "author",
            "comments",
            "contact",
            "contact_person",
            "contributors",
            "data_telegram_setting",
            "date",
            "dependencies",
            "institute",
            "institution",
            "licence",
            "processing_date",
            "project_name",
            "sensor_id",
            "sensor_name",
            "sensor_serial_number",
            "sensor_type",
            "site_name",
            "station_altitude",
            "station_latitude",
            "station_longitude",
            "station_name",
        }
        for attr in self.nc.ncattrs():
            if attr.lower() in ignore_case_insensitive or (
                attr.lower() in keep_case_sensitive and attr not in keep_case_sensitive
            ):
                delattr(self.nc, attr)

    def copy_variable(self, key: str, time_ind: list | None = None):
        """Copies one variable from Parsivel source file to target.
        Optionally uses certain time indices only.
        """
        if key not in self.nc_raw.variables.keys():
            logging.warning(f"Key {key} not found from the source file.")
            return
        variable = self.nc_raw.variables[key]
        dtype = variable.dtype
        dtype = "f4" if dtype == "f8" else dtype
        keymap = {
            "time": "f8",
            "T_sensor": "f4",
            "data_raw": "int16",
            "diameter_bnds": "f4",
            "code_4680": "int32",
            "code_4677": "int32",
            "state_sensor": "int32",
            "error_code": "int32",
        }
        if key in keymap:
            dtype = keymap[key]

        var_out = self.nc.createVariable(
            key,
            dtype,
            variable.dimensions,
            zlib=True,
            fill_value=getattr(variable, "_FillValue", None),
        )
        self._copy_variable_attributes(variable, var_out)
        screened_data = self._screen_data(variable, time_ind)

        if key in ("T_sensor", "T_pcb", "T_L_sensor_head", "T_R_sensor_head"):
            screened_data = temperature_to_k(screened_data)

        if key in (
            "rainfall_rate",
            "rain_intensity",
            "snowfall_intensity",
            "fieldV",
        ):
            screened_data = to_ms1(
                key,
                screened_data,
                variable.units if hasattr(variable, "units") else "m s-1",
            )

        if key in ("diameter_center_classes", "diameter_spread"):
            screened_data = to_m(key, screened_data, variable.units)

        var_out[:] = screened_data

    def fix_long_names(self):
        keymap = {
            "diameter_bnds": "Diameter bounds",
            "velocity_bnds": "Velocity bounds",
            "synop_WaWa": "Synop code WaWa",
            "synop_WW": "Synop code WW",
            "T_sensor": "Temperature in the sensor housing",
            "sig_laser": "Signal amplitude of the laser strip",
            "rainfall_rate": "Rainfall rate",
            "V_power_supply": "Power supply voltage",
            "velocity_spread": "Width of velocity interval",
            "diameter_spread": "Width of diameter interval",
            "data_raw": "Raw data as a function of particle diameter and velocity",
            "radar_reflectivity": "Equivalent radar reflectivity factor",
            "kinetic_energy": "Kinetic energy of the hydrometeors",
            "state_sensor": "State of the sensor",
            "I_heating": "Heating current",
            "visibility": "Meteorological optical range (MOR) visibility",
            "n_particles": "Number of particles in time interval",
            "snowfall_rate": "Snowfall rate",
            "fall_velocity": "Average velocity of each diameter class",
            "diameter": "Center diameter of precipitation particles",
        }
        self.fix_attribute(keymap, "long_name")
        skip = ("time", "visibility", "synop_WaWa", "synop_WW")
        for key, var in self.nc.variables.items():
            if key not in skip:
                var.long_name = var.long_name.lower().capitalize()

    def fix_units(self):
        keymap = {
            "velocity_spread": "m s-1",
            "velocity_bnds": "m s-1",
            "number_concentration": "m-3 mm-1",
            "kinetic_energy": "J m-2 h-1",
            "data_raw": "1",
            "n_particles": "1",
            "radar_reflectivity": "dBZ",
            "sig_laser": "1",
            "state_sensor": "1",
            "synop_WaWa": "1",
            "rainfall_rate": "m s-1",
            "snowfall_rate": "m s-1",
            "fall_velocity": "m s-1",
            "T_sensor": "K",
            "T_pcb": "K",
            "T_L_sensor_head": "K",
            "T_R_sensor_head": "K",
            "synop_WW": "1",
            "diameter": "m",
            "diameter_spread": "m",
        }
        self.fix_attribute(keymap, "units")

    def fix_standard_names(self):
        for key in self.nc.variables:
            if key not in (
                "time",
                "rainfall_rate",
                "altitude",
                "latitude",
                "longitude",
            ):
                if hasattr(self.nc.variables[key], "standard_name"):
                    delattr(self.nc.variables[key], "standard_name")
        keymap = {
            "visibility": "visibility_in_air",
            "radar_reflectivity": "equivalent_reflectivity_factor",
            "rainfall_rate": "rainfall_rate",
        }
        self.fix_attribute(keymap, "standard_name")

    def fix_comments(self):
        for key, item in ATTRIBUTES.items():
            if key not in self.nc.variables:
                continue
            if item.comment:
                self.nc.variables[key].comment = item.comment
            else:
                if hasattr(self.nc.variables[key], "comment"):
                    delattr(self.nc.variables[key], "comment")


class Ws(Level1Nc):
    def copy_ws_file_contents(self, time_ind: list):
        self.nc.createDimension("time", len(time_ind))
        for name, var_in in self.nc_raw.variables.items():
            dim = var_in.dimensions
            dtype = var_in.dtype
            if name == "time":
                dtype = "f8"
            elif len(var_in[:]) == 1:
                dim = ()
            var = self.nc.createVariable(
                name,
                dtype,
                dim,
                zlib=True,
                fill_value=getattr(var_in, "_FillValue", None),
            )
            self._copy_variable_attributes(var_in, var)
            var[:] = var_in[time_ind] if "time" in var_in.dimensions else var_in[:]

    def fix_time(self):
        """Fixes time units."""
        time = self.nc.variables["time"][:]
        ind = time > 0
        self.nc.variables["time"][ind] = time[ind] / 60
        self.nc.variables["time"].units = self._get_time_units()
        self.nc.variables["time"].calendar = "standard"

    def fix_variable_names(self):
        keymap = {
            "lat": "latitude",
            "lon": "longitude",
            "pa": "air_pressure",
            "ta": "air_temperature",
            "td": "dew_point_temperature",
            "vis": "visibility",
            "precip": "precipitation",
            "wspeed": "wind_speed",
            "wdir": "wind_direction",
            "wdir_flag": "wind_direction_quality_flag",
            "wspeed_gust": "wind_speed_gust",
            "wspeed_gust_flag": "wind_speed_gust_quality_flag",
            "pa_flag": "air_pressure_quality_flag",
            "ta_flag": "air_temperature_quality_flag",
            "td_flag": "dew_point_temperature_quality_flag",
            "hur": "relative_humidity",
            "hur_flag": "relative_humidity_quality_flag",
            "vis_flag": "visibility_quality_flag",
            "wspeed_flag": "wind_speed_quality_flag",
            "precipitation": "rainfall_rate",
            "precip_flag": "rainfall_rate_quality_flag",
        }
        self.fix_name(keymap)

    def fix_long_names(self):
        keymap = {
            "air_pressure_quality_flag": "Air pressure quality flag",
            "air_temperature_quality_flag": "Air temperature quality flag",
            "dew_point_temperature_quality_flag": "Dew point temperature quality flag",
            "relative_humidity_quality_flag": "Relative humidity quality flag",
            "wind_direction_quality_flag": "Wind direction quality flag",
            "wind_speed_gust_quality_flag": "Wind speed gust quality flag",
            "wind_speed_gust": "Wind speed gust",
            "visibility_quality_flag": "Visibility quality flag",
            "dew_point_temperature": "Dew point temperature",
            "wind_speed_quality_flag": "Wind speed quality flag",
            "rainfall_rate_quality_flag": "Rainfall rate quality flag",
            "visibility": "Meteorological optical range (MOR) visibility",
            "rainfall_amount": "Rainfall amount",
        }
        self.fix_attribute(keymap, "long_name")

    def fix_standard_names(self):
        keymap = {
            "visibility": "visibility_in_air",
            "rainfall_amount": "thickness_of_rainfall_amount",
            "dew_point_temperature": "dew_point_temperature",
            "wind_speed_gust": "wind_speed_of_gust",
        }
        self.fix_attribute(keymap, "standard_name")

    def fix_ancillary_variable_names(self):
        for key, var in self.nc.variables.items():
            name = f"{key}_quality_flag"
            if hasattr(var, "ancillary_variables") and name in self.nc.variables:
                var.ancillary_variables = name

    def fix_rainfall_variables(self):
        orig_data = self.nc["rainfall_rate"][:]
        dt = np.median(np.diff(self.nc.variables["time"][:]))
        rainfall_rate = to_ms1("rainfall_rate", orig_data / dt, "mm h-1")
        self.nc["rainfall_rate"][:] = rainfall_rate
        self.nc.createVariable("rainfall_amount", "f4", ("time",), zlib=True)
        self.nc["rainfall_amount"][:] = np.cumsum(orig_data) / 1e3  # mm -> m
        self.nc["rainfall_amount"].units = "m"

    def fix_flag_attributes(self):
        for key, var in self.nc.variables.items():
            if key.endswith("_flag") and hasattr(var, "flag_values"):
                var.flag_values = np.array(var.flag_values, dtype="i1")
                var.units = "1"


def to_ms1(variable: str, data: np.ndarray, units: str) -> np.ndarray:
    if units.lower() in ("mm h", "mm h-1", "mm/h", "mm / h"):
        logging.info(f'Converting {variable} from "{units}" to "m s-1".')
        data /= 1000 * 3600
    elif units.lower() not in ("m s-1", "m/s", "m / s"):
        raise ValueError(f"Unsupported unit {units} in variable {variable}")
    return data


def to_m(variable: str, data: np.ndarray, units: str) -> np.ndarray:
    if units.lower() == "mm":
        logging.info(f'Converting {variable} from "{units}" to "m".')
        data /= 1000
    elif units.lower() != "m":
        raise ValueError(f"Unsupported unit {units} in variable {variable}")
    return data


def temperature_to_k(data: np.ndarray) -> np.ndarray:
    data = np.array(data, dtype="float32")
    temperature_limit = 100
    ind = np.where(data < temperature_limit)
    if len(ind[0]) > 0:
        logging.info('Converting temperature from "C" to "K".')
        data[ind] += 273.15
    return data
