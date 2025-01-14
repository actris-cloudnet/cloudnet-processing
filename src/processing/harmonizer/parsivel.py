import logging
import shutil
from tempfile import NamedTemporaryFile

import netCDF4
import numpy as np
from cloudnetpy.instruments import instruments
from cloudnetpy.instruments.disdrometer import ATTRIBUTES

from processing.harmonizer import core


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
    if "output_path" not in data:
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


class ParsivelNc(core.Level1Nc):
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
            screened_data = core.to_ms1(
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


def temperature_to_k(data: np.ndarray) -> np.ndarray:
    data = np.array(data, dtype="float32")
    temperature_limit = 100
    ind = np.where(data < temperature_limit)
    if len(ind[0]) > 0:
        logging.info('Converting temperature from "C" to "K".')
        data[ind] += 273.15
    return data


def to_m(variable: str, data: np.ndarray, units: str) -> np.ndarray:
    if units.lower() == "mm":
        logging.info(f'Converting {variable} from "{units}" to "m".')
        data /= 1000
    elif units.lower() != "m":
        raise ValueError(f"Unsupported unit {units} in variable {variable}")
    return data
