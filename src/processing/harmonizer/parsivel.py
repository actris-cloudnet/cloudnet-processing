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
}


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
                "M",
                "status_sensor",
                "ved_class",
                "rof_class",
                "N",
                "rain_accum",
                "vclasses",
                "dclasses",
                "vwidth",
                "dwidth",
                "serial_no",
                "dwidth",
                "snow_intensity",
            ),
        )
        parsivel.fix_variable_names()
        parsivel.convert_time()
        parsivel.convert_precipitations()
        parsivel.convert_diameters()
        parsivel.clean_global_attributes()
        parsivel.add_date()
        parsivel.add_geolocation()
        parsivel.add_global_attributes("disdrometer", instruments.PARSIVEL2)
        parsivel.add_history("disdrometer", source="History")
        parsivel.fix_long_names()
        parsivel.fix_units()
        parsivel.fix_standard_names()
        parsivel.fix_comments()
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

        if key == "time":
            dtype = "f8"
        elif key in ("diameter_bnds",):
            dtype = "f4"
        elif key in ("data_raw",):
            dtype = "i2"
        elif np.issubdtype(variable.dtype, np.integer):
            dtype = "i4"
        elif np.issubdtype(variable.dtype, np.floating):
            dtype = "f4"
        else:
            logging.warning(f"Skipping '{key}' - unsupported dtype {variable.dtype}")
            return

        fill_value = (
            netCDF4.default_fillvals[dtype]
            if isinstance(variable, ma.MaskedArray)
            else None
        )

        dimensions = tuple(DIMENSION_MAP.get(dim, dim) for dim in variable.dimensions)

        var_out = self.nc.createVariable(
            key,
            dtype,
            dimensions,
            zlib=True,
            fill_value=fill_value,
        )
        self._copy_variable_attributes(variable, var_out)
        var_out[:] = self._screen_data(variable, time_ind)

    def fix_variable_names(self):
        keymap = {
            "V_sensor": "V_power_supply",
            "E_kin": "kinetic_energy",
            "Synop_WaWa": "synop_WaWa",
            "wawa": "synop_WaWa",
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
            "rr": "rainfall_rate",
            "Ze": "radar_reflectivity",
        }
        self.fix_name(keymap)

    def add_serial_number(self):
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
            "v": "Doppler velocity",
            "interval": "Length of measurement interval",
            "velocity": "Center fall velocity of precipitation particles",
            "number_concentration": "Number of particles per diameter class",
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

    def convert_precipitations(self):
        for key in self.nc.variables:
            if key in (
                "rainfall_rate",
                "snowfall_rate",
                "fall_velocity",
            ):
                self.to_ms1(key)

    def convert_diameters(self):
        for key in self.nc.variables:
            if key in ("diameter", "diameter_spread"):
                self.to_m(key)
