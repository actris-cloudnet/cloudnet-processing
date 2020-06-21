"""Module containing helper functions for CH15k concatenation."""
import os
import re
from datetime import datetime
import numpy as np
import netCDF4
from cloudnetpy import utils


def get_default_range(period):
    """Returns default time ranges."""
    if period == 'year':
        span = [2000, datetime.now().year]
    elif period == 'month':
        span = [1, 12]
    elif period == 'day':
        span = [1, 31]
    else:
        raise ValueError('Invalid period.')
    return span


def get_dirs_in_range(the_path, the_range):
    """Finds directories inside given (time) range."""
    ind0, ind1 = the_range
    dirs = os.listdir(the_path)
    return [d for d in dirs if os.path.isdir(os.path.join(the_path, d))
            and d.isdigit() and int(d) in np.arange(ind0, ind1+1)]


def get_full_input_path(input_dir, date):
    """Builds correct sub folder structure."""
    return os.path.join(input_dir, *date)


def get_list_of_nc_files(directory):
    """Returns sorted list of .nc-files."""
    files = os.listdir(directory)
    files = np.sort(get_only_nc_files(files))
    return [os.path.join(directory, f) for f in files]


def remove_files_with_wrong_date(files, date):
    """Remove files that contain wrong date and thus are in wrong folder."""
    date_as_ints = [int(x) for x in date]
    valid_files = []
    for file in files:
        nc = netCDF4.Dataset(file)
        if _validate_date_attributes(nc, date_as_ints):
            valid_files.append(file)
        nc.close()
    return valid_files


def _validate_date_attributes(obj, date):
    for ind, attr in enumerate(('year', 'month', 'day')):
        if getattr(obj, attr) != date[ind]:
            return False
    return True


def get_only_nc_files(files):
    """Returns only .nc files"""
    return [f for f in files if f.endswith('.nc')]


def get_dtype(key, array):
    """Returns correct data type for array."""
    if key == 'time':
        return 'f8'
    if 'int' in str(array.dtype):
        return 'i4'
    return 'f4'


def get_dim(file, array):
    """Returns tuple of dimension names, e.g., (time, range) that match the array size."""
    if utils.isscalar(array):
        return ()
    variable_size = ()
    file_dims = file.dimensions
    for length in array.shape:
        try:
            dim = [key for key in file_dims.keys()
                   if file_dims[key].size == length][0]
        except IndexError:
            dim = 'time'
        variable_size = variable_size + (dim,)
    return variable_size


def find_date(file):
    """Finds measurement date."""
    folders = file.split('/')
    date = (_parse_year(folders[-4]),
            _parse_month(folders[-3]),
            _parse_day(folders[-2]))
    if all(date):
        return date
    return _search_date_from_filename(folders[-1])


def _search_date_from_filename(name):
    for i, _ in enumerate(name):
        part = name[i:i+8]
        date = (_parse_year(part[:4]),
                _parse_month(part[4:6]),
                _parse_day(part[6:8]))
        if all(date):
            return date
    return None


def _parse_year(string):
    if len(string) == 5:
        string = string[1:]
    if re.compile(r'[1-2][0-9]{3}').match(string) is not None:
        return string
    return None


def _parse_month(string):
    if len(string) == 3:
        string = string[1:]
    if re.compile(r'^(0?[1-9]|1[012])$').match(string) is not None:
        return string
    return None


def _parse_day(string):
    if len(string) == 3:
        string = string[1:]
    if re.compile(r'(0[1-9]|[12]\d|3[01])').match(string) is not None:
        return string
    return None
