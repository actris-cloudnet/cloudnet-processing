import shutil
from tempfile import NamedTemporaryFile
from uuid import UUID

import netCDF4

from processing.harmonizer import core
from processing.utils import MiscError


def harmonize_model_file(data: dict) -> UUID:
    """Harmonizes model netCDF file."""
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
        model = ModelNc(nc_raw, nc, data)
        model.copy_file_contents()
        model.harmonize_attribute("units", ("latitude", "longitude", "altitude"))
        uuid = model.add_uuid()
        model.add_global_model_attributes()
        model.check_time_dimension()
        model.add_date()
        model.add_history("model")
    if "output_path" not in data:
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


class ModelNc(core.Level1Nc):
    def check_time_dimension(self) -> None:
        """Checks time dimension."""
        time = self.nc["time"][:]
        for time_step in range(0, 24, 3):
            if time_step not in time:
                raise MiscError(f"Time step {time_step} not in model file")

    def add_date(self) -> None:
        """Adds date in correct format."""
        date_string = self.nc.variables["time"].units
        date = date_string.split()[2]
        self.nc.year, self.nc.month, self.nc.day = date.split("-")

    def add_global_model_attributes(self) -> None:
        """Adds required global attributes."""
        self.nc.cloudnet_file_type = "model"
        self.nc.Conventions = "CF-1.8"
