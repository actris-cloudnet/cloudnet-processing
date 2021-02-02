"""Module containing helper functions for CH15k concatenation."""
import netCDF4
from cloudnetpy import concat_lib as clib


def concat_chm15k_files(files: list, date: str, output_file: str) -> list:
    """ Concatenate several small chm15k files into a daily file.

    Args:
        files (list): List of file to be concatenated.
        date (str): Measurement date 'YYYY-MM-DD'.
        output_file (str): Output file name, e.g., 20201012_bucharest_chm15k.nc.

    Returns:
        list: List of files that were valid and actually used in the concatenation.

    Raises:
        ValueError: No valid files to be concatenated.

    """
    valid_files = _remove_files_with_wrong_date(files, date)
    if len(valid_files) == 0:
        raise ValueError
    variables = ['time', 'beta_raw', 'stddev', 'nn1', 'nn2', 'nn3']
    clib.concatenate_files(valid_files, output_file, variables=variables,
                           new_attributes={'Conventions': 'CF-1.7'})
    return valid_files


def _remove_files_with_wrong_date(files: list, date: str) -> list:
    """Remove files that contain wrong date."""
    date = date.split('-')
    date_as_ints = [int(x) for x in date]
    valid_files = []
    for file in files:
        nc = netCDF4.Dataset(file)
        if _validate_date_attributes(nc, date_as_ints):
            valid_files.append(file)
        nc.close()
    return valid_files


def _validate_date_attributes(obj: netCDF4.Dataset, date: list) -> bool:
    for ind, attr in enumerate(('year', 'month', 'day')):
        if getattr(obj, attr) != date[ind]:
            return False
    return True
