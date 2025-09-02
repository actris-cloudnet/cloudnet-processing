import logging
import shutil
from tempfile import NamedTemporaryFile
from uuid import UUID

import netCDF4
import numpy as np
import numpy.ma as ma
from cloudnetpy.instruments import instruments

from processing.harmonizer import core

DIMENSION_MAP = {
    "datetime": "time",
}

VARIABLE_MAP = {
    # Lindenberg
    "pa": "air_pressure",
    "ta": "air_temperature",
    "td": "dew_point_temperature",
    "vis": "visibility",
    "precip": "rainfall_rate",
    "wdir": "wind_direction",
    "wspeed_gust": "wind_speed_gust",
    "hur": "relative_humidity",
    "wspeed": "wind_speed",
    "wdir_flag": "wind_direction_quality_flag",
    "wspeed_gust_flag": "wind_speed_gust_quality_flag",
    "pa_flag": "air_pressure_quality_flag",
    "hur_flag": "relative_humidity_quality_flag",
    "vis_flag": "visibility_quality_flag",
    "wspeed_flag": "wind_speed_quality_flag",
    "precip_flag": "rainfall_rate_quality_flag",
    "ta_flag": "air_temperature_quality_flag",
    "td_flag": "dew_point_temperature_quality_flag",
    # Munich
    "Regen": "rainfall_rate",
    "Luftt._2m": "air_temperature",
    "Luftdruck": "air_pressure",
    "Rel.Feuchte_2m": "relative_humidity",
    "Regensumme": "rainfall_amount",
    "Richt_30m": "wind_direction",
    "Wind_30m": "wind_speed",
}


def harmonize_ws_file(data: dict) -> UUID:
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
        ws.fix_name(VARIABLE_MAP)
        ws.clean_global_attributes()
        ws.add_global_attributes("weather-station", instruments.GENERIC_WEATHER_STATION)
        ws.add_geolocation()
        ws.add_date()
        uuid = ws.add_uuid()
        ws.clean_variable_attributes(
            accepted_extra=("flag_values", "flag_meanings", "ancillary_variables")
        )
        for attribute in ("long_name", "standard_name"):
            ws.harmonize_attribute(attribute)
        ws.add_history("weather-station")
        ws.nc.source = "Weather station"
        ws.convert_time()
        ws.convert_rainfall_rate()
        if "rainfall_amount" not in ws.nc.variables:
            ws.calculate_rainfall_amount()
        ws.to_k("air_temperature")
        ws.to_pa("air_pressure")
        ws.to_ratio("relative_humidity")
        ws.to_m("rainfall_amount")
        ws.to_ms1("wind_speed")
        ws.to_degree("wind_direction")
        ws.fix_standard_names()
        ws.fix_long_names()
        ws.fix_comments()
        ws.fix_flag_attributes()
        ws.fix_ancillary_variable_names()
        ws.mask_bad_data()
    if "output_path" not in data:
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


class Ws(core.Level1Nc):
    def copy_ws_file_contents(self, time_ind: list) -> None:
        self.nc.createDimension("time", len(time_ind))
        for key, variable in self.nc_raw.variables.items():
            if key not in list(VARIABLE_MAP.keys()) + ["datetime", "time"]:
                continue

            key = "time" if key == "datetime" else key

            dimensions: tuple[str, ...]

            if len(variable[:]) == 1:
                dimensions = ()
            else:
                dimensions = tuple(
                    DIMENSION_MAP.get(dim, dim) for dim in variable.dimensions
                )

            if key == "time":
                dtype = "f8"
            elif "flag" in key and variable.dtype == "int8":
                dtype = "i1"
            elif np.issubdtype(variable.dtype, np.integer):
                dtype = "i4"
            elif np.issubdtype(variable.dtype, np.floating):
                dtype = "f4"
            else:
                logging.warning(
                    f"Skipping '{key}' - unsupported dtype {variable.dtype}"
                )
                return

            data = variable[time_ind] if "time" in dimensions else variable[:]
            fill_value = _get_fill_value(data)

            var = self.nc.createVariable(
                key, dtype, dimensions, zlib=True, fill_value=fill_value
            )
            self._copy_variable_attributes(variable, var)
            var[:] = data

    def fix_long_names(self) -> None:
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

    def fix_standard_names(self) -> None:
        keymap = {
            "visibility": "visibility_in_air",
            "rainfall_amount": "thickness_of_rainfall_amount",
            "dew_point_temperature": "dew_point_temperature",
            "wind_speed_gust": "wind_speed_of_gust",
        }
        self.fix_attribute(keymap, "standard_name")

    def fix_ancillary_variable_names(self) -> None:
        for key, var in self.nc.variables.items():
            name = f"{key}_quality_flag"
            if hasattr(var, "ancillary_variables") and name in self.nc.variables:
                var.ancillary_variables = name

    def convert_rainfall_rate(self) -> None:
        """Converts rainfall rate to correct units."""
        orig_data = self.nc["rainfall_rate"][:]
        orig_units = self.nc["rainfall_rate"].units
        dt = np.median(np.diff(self.nc.variables["time"][:]))
        # In Lindenberg, "rate" is actually given as amount -> convert to true rainfall rate
        if orig_units in ("kg m-2", "mm"):
            self.nc["rainfall_rate"][:] = orig_data / dt  # mm -> mm h-1
            self.nc["rainfall_rate"].units = "mm h-1"
        self.to_ms1("rainfall_rate")

    def calculate_rainfall_amount(self) -> None:
        """Calculates rainfall amount from rainfall rate."""
        dt = np.median(np.diff(self.nc.variables["time"][:]))
        rate = self.nc["rainfall_rate"]
        fill_value = _get_fill_value(rate)
        self.nc.createVariable(
            "rainfall_amount", "f4", ("time",), zlib=True, fill_value=fill_value
        )
        if rate.units != "m s-1":
            raise ValueError("Rainfall rate units are not m s-1.")
        self.nc["rainfall_amount"][:] = np.cumsum(rate[:]) * dt * 3600
        self.nc["rainfall_amount"].units = "m"

    def fix_comments(self) -> None:
        if "rainfall_amount" in self.nc.variables:
            self.nc[
                "rainfall_amount"
            ].comment = "Cumulated precipitation since 00:00 UTC"

    def fix_flag_attributes(self) -> None:
        for key, var in self.nc.variables.items():
            if key.endswith("_flag") and hasattr(var, "flag_values"):
                var.flag_values = np.array(var.flag_values, dtype="i1")
                var.units = "1"

    def mask_bad_data(self) -> None:
        for key, var in self.nc.variables.items():
            if flagvar := self.nc.variables.get(f"{key}_quality_flag"):
                var[:] = ma.masked_where(flagvar[:] == 1, var[:])
            var[:] = ma.masked_invalid(var[:])


def _get_fill_value(data: np.ndarray) -> int | float | str | None:
    if hasattr(data, "_FillValue") and data._FillValue != -999.0:
        return data._FillValue
    if isinstance(data, ma.MaskedArray):
        dtype = data.dtype.str[1:]
        return netCDF4.default_fillvals[dtype]
    return None
