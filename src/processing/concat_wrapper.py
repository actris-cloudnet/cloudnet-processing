"""Module containing helper functions for CH15k concatenation."""

import datetime
import shutil
from pathlib import Path

import netCDF4
from cloudnetpy import concat_lib as clib
from cloudnetpy.utils import get_epoch, seconds2date


def concat_netcdf_files(
    files: list[Path],
    date: datetime.date,
    output_file: Path,
    concat_dimension: str = "time",
    variables: list | None = None,
) -> list[Path]:
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
            if seconds2date(timestamp, epoch).date() == date:
                valid_files.append(file)
                break

    clib.concatenate_files(
        valid_files,
        output_file,
        concat_dimension=concat_dimension,
        variables=variables,
    )
    return valid_files


def concat_chm15k_files(
    files: list[Path], date: datetime.date, output_file: Path
) -> list:
    """Concatenate several small chm15k files into a daily file.

    Args:
        files: list of file to be concatenated.
        date: Measurement date.
        output_file: Output file name, e.g., 20201012_bucharest_chm15k.nc.

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
    )
    return valid_files


def _remove_files_with_wrong_date(files: list, date: datetime.date) -> list:
    """Remove files that contain wrong date."""
    valid_files = []
    for file in files:
        with netCDF4.Dataset(file) as nc:
            if _validate_date_attributes(nc, (date.year, date.month, date.day)):
                valid_files.append(file)
    return valid_files


def _validate_date_attributes(obj: netCDF4.Dataset, date: tuple[int, int, int]) -> bool:
    for ind, attr in enumerate(("year", "month", "day")):
        if getattr(obj, attr) != date[ind]:
            return False
    return True
