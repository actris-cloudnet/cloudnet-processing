"""Module containing helper functions for CH15k concatenation."""
import netCDF4
from cloudnetpy import utils


CONSTANTS = ['range', 'wavelength', 'scaling', 'zenith']
VARIABLES = ['time', 'beta_raw', 'stddev', 'nn1', 'nn2', 'nn3']


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

    files.sort()
    valid_files = _remove_files_with_wrong_date(files, date)
    if len(valid_files) == 0:
        raise ValueError
    file_new = netCDF4.Dataset(output_file, 'w', format='NETCDF4_CLASSIC')
    first_file_of_day = netCDF4.Dataset(valid_files[0])
    _create_dimensions(file_new, first_file_of_day)
    _create_global_attributes(file_new, first_file_of_day)
    _write_initial_data(file_new, first_file_of_day)

    if len(valid_files) > 1:
        for file in valid_files[1:]:
            _append_data(file_new, netCDF4.Dataset(file))

    file_new.close()
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


def _create_dimensions(file_new: netCDF4.Dataset, file_source: netCDF4.Dataset) -> None:
    n_range = len(file_source['range'])
    file_new.createDimension('time', None)
    file_new.createDimension('range', n_range)


def _create_global_attributes(file_new: netCDF4.Dataset, file_source: netCDF4.Dataset) -> None:
    file_new.Conventions = 'CF-1.7'
    _copy_attributes(file_source, file_new)


def _copy_attributes(source: netCDF4.Dataset, target: netCDF4.Dataset) -> None:
    for attr in source.ncattrs():
        value = getattr(source, attr)
        setattr(target, attr, value)


def _write_initial_data(file_new: netCDF4.Dataset, file_source: netCDF4.Dataset) -> None:
    for key in CONSTANTS + VARIABLES:
        array = file_source[key][:]
        var = file_new.createVariable(key, _get_dtype(key, array), _get_dim(file_new, array),
                                      zlib=True, complevel=3, shuffle=False)
        var[:] = array
        _copy_attributes(file_source[key], var)


def _append_data(file_base: netCDF4.Dataset, file: netCDF4.Dataset) -> None:
    ind0 = len(file_base.variables['time'])
    ind1 = ind0 + len(file.variables['time'])
    for key in VARIABLES:
        array = file[key][:]
        if array.ndim == 1:
            file_base.variables[key][ind0:ind1] = array
        else:
            file_base.variables[key][ind0:ind1, :] = array


def _get_dtype(key: str, array) -> str:
    """Returns correct data type for array."""
    if key == 'time':
        return 'f8'
    if 'int' in str(array.dtype):
        return 'i4'
    return 'f4'


def _get_dim(file: netCDF4.Dataset, array) -> tuple:
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
