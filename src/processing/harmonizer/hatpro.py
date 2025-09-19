import datetime
import shutil
from tempfile import NamedTemporaryFile
from uuid import UUID

import cloudnetpy.exceptions
import cloudnetpy.utils
import netCDF4
import numpy as np
from cloudnetpy.instruments import instruments

from processing.harmonizer import core
from processing.utils import MiscError, utctoday


def harmonize_hatpro_file(data: dict) -> UUID:
    """Harmonizes calibrated HATPRO netCDF file."""
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
    if "output_path" not in data:
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


class HatproNc(core.Level1Nc):
    bad_lwp_keys = ("LWP", "LWP_data", "clwvi", "atmosphere_liquid_water_content")

    def copy_file(self, all_keys: bool = False) -> None:
        """Copies essential fields only."""
        valid_ind = self._get_valid_timestamps()
        if all_keys is True:
            possible_keys = None
        else:
            possible_keys = ("lwp", "time") + self.bad_lwp_keys
        self._copy_hatpro_file_contents(valid_ind, possible_keys)

    def add_lwp(self) -> None:
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

    def check_lwp_data(self) -> None:
        """Sanity checks LWP data."""
        threshold_kg = 10
        lwp = self.nc.variables["lwp"][:]
        positive_lwp_values = lwp[lwp > 0]
        if (median_value := np.median(positive_lwp_values)) > threshold_kg:
            raise MiscError(
                f"Invalid LWP data, median value: {np.round(median_value, 2)} kg"
            )

    def sort_time(self) -> None:
        """Sorts time array."""
        time = self.nc.variables["time"][:]
        array = self.nc.variables["lwp"][:]
        ind = time.argsort()
        self.nc.variables["time"][:] = time[ind]
        self.nc.variables["lwp"][:] = array[ind]

    def check_time_reference(self) -> None:
        """Checks the reference time zone."""
        key = "time_reference"
        if key in self.nc_raw.variables:
            if self.nc_raw.variables[key][:] != 1:  # not UTC
                raise ValueError("Local time is not supported")

    def _get_valid_timestamps(self) -> list:
        time_stamps = self.nc_raw.variables["time"][:]
        epoch = _get_epoch(self.nc_raw.variables["time"].units)
        expected_date = self.data["date"]
        valid_ind = []
        for t_ind, t in enumerate(time_stamps):
            if (0 < t < 24 and epoch.date() == expected_date) or (
                cloudnetpy.utils.seconds2date(t, epoch).date() == expected_date
            ):
                valid_ind.append(t_ind)
        if not valid_ind:
            raise cloudnetpy.exceptions.ValidTimeStampError
        _, ind = np.unique(time_stamps[valid_ind], return_index=True)
        return list(np.array(valid_ind)[ind])

    def _copy_hatpro_file_contents(
        self, time_ind: list, keys: tuple | None = None
    ) -> None:
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


def _get_epoch(units: str) -> datetime.datetime:
    fallback = datetime.datetime(2001, 1, 1, tzinfo=datetime.timezone.utc)
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
    current_year = utctoday().year
    if (1900 < year <= current_year) and (0 < month < 13) and (0 < day < 32):
        return datetime.datetime(year, month, day, tzinfo=datetime.timezone.utc)
    return fallback
