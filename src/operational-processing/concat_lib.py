import os
from datetime import datetime
import numpy as np
from cloudnetpy import utils


def get_default_range(period):
    if period == 'year':
        return [2000, datetime.now().year]
    elif period == 'month':
        return [1, 12]
    elif period == 'day':
        return [1, 31]


def get_good_dirs(the_path, the_range):
    ind0, ind1 = the_range
    dirs = os.listdir(the_path)
    return [d for d in dirs if os.path.isdir('/'.join((the_path, d)))
            and d.isdigit() and int(d) in np.arange(ind0, ind1+1)]


def get_full_input_path(input_dir, date):
    return '/'.join((input_dir, *date))


def get_files_for_day(full_input_dir):
    """Returns sorted list of .nc-files."""
    files = os.listdir(full_input_dir)
    files = np.sort(get_files_by_suffix(files))
    return ['/'.join((full_input_dir, f)) for f in files]


def get_files_by_suffix(files):
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
