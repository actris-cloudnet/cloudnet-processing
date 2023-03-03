"""Module containing helper functions for CH15k concatenation."""
import logging
import shutil

import netCDF4
from cloudnetpy import concat_lib as clib
from cloudnetpy.utils import get_epoch, seconds2date


def update_daily_file(new_files: list, daily_file: str) -> list:
    """Appends new files to existing daily file."""
    if not new_files:
        return []
    valid_files = []
    new_files.sort()
    for file in new_files:
        success = clib.update_nc(daily_file, file)
        if success == 1:
            valid_files.append(file)
    logging.info(f"Added {len(valid_files)} new files")
    return valid_files


def concat_netcdf_files(
    files: list,
    date: str,
    output_file: str,
    concat_dimension: str = "time",
    variables: list | None = None,
) -> list:
    """Concatenates several netcdf files into daily file."""
    with netCDF4.Dataset(files[0]) as nc:
        if concat_dimension not in nc.dimensions:
            raise KeyError
    if len(files) == 1:
        shutil.copy(files[0], output_file)
        return files
    valid_files = []
    for file in files:
        try:
            with netCDF4.Dataset(file) as nc:
                time = nc.variables["time"]
                time_array = time[:]
                time_units = time.units
        except OSError:
            continue
        epoch = get_epoch(time_units)
        for timestamp in time_array:
            if seconds2date(timestamp, epoch)[:3] == date.split("-"):
                valid_files.append(file)
                break
    clib.concatenate_files(
        valid_files,
        output_file,
        concat_dimension=concat_dimension,
        variables=variables,
        ignore=[
            "minimum",
            "maximum",
            "number_integrated_samples",
            "Min_LWP",
            "Max_LWP",
        ],
    )
    return valid_files


def concat_chm15k_files(files: list, date: str, output_file: str) -> list:
    """Concatenate several small chm15k files into a daily file.

    Args:
        files (list): list of file to be concatenated.
        date (str): Measurement date 'YYYY-MM-DD'.
        output_file (str): Output file name, e.g., 20201012_bucharest_chm15k.nc.

    Returns:
        list: list of files that were valid and actually used in the concatenation.

    Raises:
        ValueError: No valid files to be concatenated.

    """
    valid_files = _remove_files_with_wrong_date(files, date)
    if len(valid_files) == 0:
        raise ValueError
    variables = ["time", "beta_raw", "stddev", "nn1", "nn2", "nn3", "beta_att"]
    clib.concatenate_files(
        valid_files,
        output_file,
        variables=variables,
        new_attributes={"Conventions": "CF-1.8"},
    )
    return valid_files


def _remove_files_with_wrong_date(files: list, date_str: str) -> list:
    """Remove files that contain wrong date."""
    date = date_str.split("-")
    date_as_ints = [int(x) for x in date]
    valid_files = []
    for file in files:
        with netCDF4.Dataset(file) as nc:
            if _validate_date_attributes(nc, date_as_ints):
                valid_files.append(file)
    return valid_files


def _validate_date_attributes(obj: netCDF4.Dataset, date: list) -> bool:
    for ind, attr in enumerate(("year", "month", "day")):
        if getattr(obj, attr) != date[ind]:
            return False
    return True
