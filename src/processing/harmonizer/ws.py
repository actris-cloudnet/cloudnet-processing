import shutil
from tempfile import NamedTemporaryFile

import netCDF4
import numpy as np
import numpy.ma as ma
from cloudnetpy.instruments import instruments

from processing.harmonizer import core


def harmonize_ws_file(data: dict) -> str:
    """Harmonizes weather station netCDF file."""
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
        ws.mask_bad_data()
    if "output_path" not in data:
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


class Ws(core.Level1Nc):
    def copy_ws_file_contents(self, time_ind: list):
        self.nc.createDimension("time", len(time_ind))
        for name, var_in in self.nc_raw.variables.items():
            dim = var_in.dimensions
            dtype = var_in.dtype.str[1:]
            if name == "time":
                dtype = "f8"
            elif len(var_in[:]) == 1:
                dim = ()
            data = var_in[time_ind] if "time" in var_in.dimensions else var_in[:]
            fill_value = _get_fill_value(data)
            var = self.nc.createVariable(
                name, dtype, dim, zlib=True, fill_value=fill_value
            )
            self._copy_variable_attributes(var_in, var)
            var[:] = data

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
        rainfall_rate = core.to_ms1("rainfall_rate", orig_data / dt, "mm h-1")
        self.nc["rainfall_rate"][:] = rainfall_rate
        fill_value = _get_fill_value(rainfall_rate)
        self.nc.createVariable(
            "rainfall_amount", "f4", ("time",), zlib=True, fill_value=fill_value
        )
        self.nc["rainfall_amount"][:] = np.cumsum(orig_data) / 1e3  # mm -> m
        self.nc["rainfall_amount"].units = "m"

    def fix_flag_attributes(self):
        for key, var in self.nc.variables.items():
            if key.endswith("_flag") and hasattr(var, "flag_values"):
                var.flag_values = np.array(var.flag_values, dtype="i1")
                var.units = "1"

    def mask_bad_data(self):
        for key, var in self.nc.variables.items():
            if flagvar := self.nc.variables.get(f"{key}_quality_flag"):
                var[:] = ma.masked_where(flagvar[:] < 2, var[:])


def _get_fill_value(data: np.ndarray) -> int | float | str | None:
    if hasattr(data, "_FillValue") and data._FillValue != -999.0:
        return data._FillValue
    if isinstance(data, ma.MaskedArray):
        dtype = data.dtype.str[1:]
        return netCDF4.default_fillvals[dtype]
    return None
