import logging
import shutil
from tempfile import NamedTemporaryFile

import netCDF4
import numpy as np
from cloudnetpy.instruments import instruments
from cloudnetpy.instruments.disdrometer import ATTRIBUTES
from numpy import ma

from processing.harmonizer import core

DIMENSION_MAP = {
    "ved_class": "velocity",
    "rof_class": "diameter",
    "diameter_classes": "diameter",
    "velocity_classes": "velocity",
    # Old Leipzig files:
    "times": "time",
    "velocities": "velocity",
    "diameters": "diameter",
}

VELOCITIES = [
    0.05,
    0.15,
    0.23,
    0.35,
    0.45,
    0.55,
    0.65,
    0.75,
    0.85,
    0.95,
    1.1,
    1.3,
    1.5,
    1.7,
    1.9,
    2.2,
    2.6,
    3,
    3.4,
    3.8,
    4.4,
    5.2,
    6,
    6.8,
    7.6,
    8.8,
    10.4,
    12,
    13.6,
    15.2,
    17.6,
    20.8,
]

DIAMETERS = [
    6.2e-05,
    0.000187,
    0.000312,
    0.000437,
    0.000562,
    0.000687,
    0.000812,
    0.000937,
    0.001062,
    0.001187,
    0.001375,
    0.001625,
    0.001875,
    0.002125,
    0.002375,
    0.00275,
    0.00325,
    0.00375,
    0.00425,
    0.00475,
    0.0055,
    0.0065,
    0.0075,
    0.0085,
    0.0095,
    0.011,
    0.013,
    0.015,
    0.017,
    0.019,
    0.0215,
    0.0245,
]


def harmonize_parsivel_file(data: dict) -> str:
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
        parsivel = ParsivelNc(nc_raw, nc, data)
        valid_ind = parsivel.get_valid_time_indices()
        parsivel.create_dimensions(valid_ind)
        parsivel.copy_file(
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
                "signal_amplitude",
                "T_sensor_housing",
                "T_sensor_left",
                "T_sensor_right",
                "T_pcb",
                "T_sensor",
                "sample_interval",
                "curr_heating",
                "volt_sensor",
                "status_sensor",
                "ved_class",
                "rof_class",
                "rain_accum",
                "vwidth",
                "dwidth",
                "serial_no",
                "dwidth",
                "snow_intensity",
                "velocity_upper_bounds",
                "velocity_lower_bounds",
                "diameter_upper_bounds",
                "diameter_lower_bounds",
                "absolute_rain_amount",
                "T_L_sensor_head",
                "T_R_sensor_head",
                # Old Leipzig files:
                "RR_Accumulated",
                "RR_Total",
            ),
        )
        if "Meas_Time" in nc_raw.variables:
            for key, var in nc.variables.items():
                if key == "time":
                    var.units = "seconds since 1970-01-01 00:00:00 +00:00"
                elif key == "data_raw":
                    var.units = "1"
                elif key == "number_concentration":
                    var.units = ("m-3 mm-1",)
                elif key == "fall_velocity":
                    var.units = "m s-1"
                elif hasattr(var, "unit"):
                    var.units = var.unit
        if "velocity" not in nc.variables:
            variable = nc.createVariable("velocity", "f4", "velocity")
            variable[:] = VELOCITIES
            variable.long_name = "Center fall velocity of precipitation particles"
            variable.units = "m s-1"
        if "diameter" not in nc.variables:
            variable = nc.createVariable("diameter", "f4", "diameter")
            variable[:] = DIAMETERS
            variable.long_name = "Center diameter of precipitation particles"
            variable.units = "m"
        parsivel.convert_time()
        parsivel.convert_velocity()
        parsivel.convert_diameters()
        parsivel.convert_temperatures()
        parsivel.clean_global_attributes()
        parsivel.add_date()
        parsivel.add_geolocation()
        parsivel.add_global_attributes("disdrometer", instruments.PARSIVEL2)
        parsivel.add_history("disdrometer", source="History")
        parsivel.fix_long_names()
        parsivel.fix_units()
        parsivel.fix_standard_names()
        parsivel.fix_comments()
        parsivel._mask_bad_values()
        uuid = parsivel.add_uuid()
        parsivel.add_serial_number()
    if "output_path" not in data:
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


class ParsivelNc(core.Level1Nc):
    def create_dimensions(self, time_ind: list):
        for name, dimension in self.nc_raw.dimensions.items():
            name = DIMENSION_MAP.get(name, name)
            n = len(time_ind) if name == "time" else dimension.size
            self.nc.createDimension(name, n)

    def copy_file(
        self,
        time_ind: list,
        skip: tuple,
    ):
        for key in self.nc_raw.variables.keys():
            if key not in skip:
                self.copy_var(key, time_ind)
        self._copy_global_attributes()

    def copy_var(self, key: str, time_ind: list):
        variable = self.nc_raw.variables[key]

        if key in ("time", "Meas_Time"):
            dtype = "f8"
        elif key in ("diameter_bnds", "T_Sensor"):
            dtype = "f4"
        elif key in ("data_raw", "M", "Data_Raw"):
            dtype = "i2"
        elif np.issubdtype(variable.dtype, np.integer):
            dtype = "i4"
        elif np.issubdtype(variable.dtype, np.floating):
            dtype = "f4"
        else:
            logging.warning(f"Skipping '{key}' - unsupported dtype {variable.dtype}")
            return

        dimensions = tuple(DIMENSION_MAP.get(dim, dim) for dim in variable.dimensions)

        var_out = self.nc.createVariable(
            self._get_new_name(key),
            dtype,
            dimensions,
            zlib=True,
            fill_value=self._fetch_fill_value(variable, dtype),
        )
        self._copy_variable_attributes(variable, var_out)
        var_out[:] = self._screen_data(variable, time_ind)

    def _fetch_fill_value(
        self, variable: netCDF4.Variable, dtype: str
    ) -> int | float | str | None:
        bad_values = self._find_bad_values(variable)
        if isinstance(variable, ma.MaskedArray) or bad_values.any():
            return netCDF4.default_fillvals[dtype]
        return None

    def _get_new_name(self, key) -> str:
        keymap = {
            "V_sensor": "V_power_supply",
            "E_kin": "kinetic_energy",
            "Synop_WaWa": "synop_WaWa",
            "wawa": "synop_WaWa",
            "rain_intensity": "rainfall_rate",
            "MOR": "visibility",
            "reflectivity": "radar_reflectivity",
            "fieldN": "number_concentration",
            "N": "number_concentration",
            "fieldV": "fall_velocity",
            "v": "fall_velocity",
            "amplitude": "sig_laser",
            "time_interval": "interval",
            "snowfall_intensity": "snowfall_rate",
            "code_4680": "synop_WaWa",
            "code_4677": "synop_WW",
            "velocity_center_classes": "velocity",
            "dclasses": "diameter",
            "vclasses": "velocity",
            "rr": "rainfall_rate",
            "Ze": "radar_reflectivity",
            "M": "data_raw",
            "diameter_center_classes": "diameter",
            # Old Leipzig files:
            "Meas_Time": "time",
            "Meas_Interval": "interval",
            "RR_Intensity": "rainfall_rate",
            "Synop_WW": "synop_WW",
            "Reflectivity": "radar_reflectivity",
            "Visibility": "visibility",
            "T_Sensor": "T_sensor",
            "Sig_Laser": "sig_laser",
            "N_Particles": "n_particles",
            "State_Sensor": "state_sensor",
            "V_Sensor": "V_power_supply",
            "I_Heating": "I_heating",
            "Error_Code": "error_code",
            "Data_Raw": "data_raw",
            "Data_N_Field": "number_concentration",
            "Data_V_Field": "fall_velocity",
        }
        return keymap.get(key, key)

    def add_serial_number(self):
        if "serial_no" in self.nc_raw.variables:
            self.nc.serial_number = str(self.nc_raw["serial_no"][0])
        for attr in ("Sensor_ID", "sensor_serial_number"):
            if hasattr(self.nc_raw, attr):
                self.nc.serial_number = getattr(self.nc_raw, attr)

    def fix_long_names(self):
        keymap = {
            "diameter_bnds": "Diameter bounds",
            "velocity_bnds": "Velocity bounds",
            "synop_WaWa": "Synop code WaWa",
            "synop_WW": "Synop code WW",
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
            "error_code": "Error code",
            "interval": "Length of measurement interval",
            "velocity": "Center fall velocity of precipitation particles",
            "number_concentration": "Number of particles per diameter class",
            "T_sensor": "Temperature in the sensor housing",
        }
        self.fix_attribute(keymap, "long_name")
        skip = ("time", "visibility", "synop_WaWa", "synop_WW")
        for key, var in self.nc.variables.items():
            if key not in skip and hasattr(var, "long_name"):
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
            "synop_WW": "1",
            "error_code": "1",
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
        for key in self.nc.variables:
            if hasattr(self.nc.variables[key], "comment"):
                delattr(self.nc.variables[key], "comment")
            if (attr := ATTRIBUTES.get(key)) and attr.comment:
                self.nc.variables[key].comment = attr.comment

    def convert_velocity(self):
        for key in self.nc.variables:
            if key in (
                "rainfall_rate",
                "snowfall_rate",
                "velocity",
                "velocity_spread",
                "fall_velocity",
            ):
                self.to_ms1(key)

    def convert_diameters(self):
        for key in self.nc.variables:
            if key in ("diameter", "diameter_spread"):
                self.to_m(key)

    def convert_temperatures(self):
        for key in self.nc.variables:
            if key in ("T_sensor"):
                self.to_k(key)

    def _find_bad_values(self, variable: netCDF4.Variable) -> np.ndarray:
        bad_value = -9.999
        threshold = 1e-3
        data = variable[:]
        return np.isclose(data, bad_value, atol=threshold)

    def _mask_bad_values(self):
        for variable in self.nc.variables.values():
            mask = self._find_bad_values(variable)
            variable[:] = ma.masked_array(variable[:], mask=mask)
