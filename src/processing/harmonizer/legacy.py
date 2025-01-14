import netCDF4

import processing.utils
from processing.harmonizer import core


def fix_legacy_file(
    legacy_file_full_path: str, target_full_path: str, data: dict
) -> str:
    """Fixes legacy netCDF file."""
    with (
        netCDF4.Dataset(legacy_file_full_path, "r") as nc_legacy,
        netCDF4.Dataset(target_full_path, "w", format="NETCDF4_CLASSIC") as nc,
    ):
        legacy = core.Level1Nc(nc_legacy, nc, data)
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
                    res = processing.utils.get_from_data_portal_api(
                        f"api/files/{source_uuid}"
                    )
                    if isinstance(res, dict) and res.get("status") != 404:
                        valid_uuids.append(source_uuid)
                legacy.nc.source_file_uuids = ", ".join(valid_uuids)

    return uuid
