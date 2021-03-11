import shutil
from typing import Optional
from tempfile import NamedTemporaryFile
import netCDF4
import datetime
from cloudnetpy.utils import get_uuid, get_time, seconds2date
from data_processing import utils
from data_processing.utils import MiscError


def fix_legacy_file(legacy_file_full_path: str, target_full_path: str) -> str:
    """Fix legacy netCDF file."""
    uuid = get_uuid()
    nc_legacy = netCDF4.Dataset(legacy_file_full_path, 'r')
    nc_new = netCDF4.Dataset(target_full_path, 'w', format='NETCDF4_CLASSIC')
    copy_file_contents(nc_legacy, nc_new)
    history = _get_history(nc_legacy)
    nc_new.file_uuid = uuid
    nc_new.history = history
    nc_legacy.close()
    nc_new.close()
    return uuid


def harmonize_nc_file(data: dict) -> str:
    """Compresses and harmonizes metadata of a "calibrated" Cloudnet netCDF file."""
    temp_file = NamedTemporaryFile()
    nc_raw = netCDF4.Dataset(data['full_path'], 'r')
    nc = netCDF4.Dataset(temp_file.name, 'w', format='NETCDF4_CLASSIC')
    if data['instrument'] == 'hatpro':
        valid_ind = _get_valid_hatpro_timestamps(data, nc_raw)
    else:
        valid_ind = None
    copy_file_contents(nc_raw, nc, valid_ind)
    uuid = data['uuid'] or get_uuid()
    nc.file_uuid = uuid
    file_type = _get_file_type(data)
    nc.cloudnet_file_type = file_type
    if file_type == 'model':
        try:
            _check_time_dimension(nc_raw, data)
        except ValueError:
            nc.close()
            nc_raw.close()
            raise MiscError('Incomplete model file.')
        nc.year, nc.month, nc.day = _get_model_date(nc)
    if data['instrument'] == 'hatpro':
        nc.year, nc.month, nc.day = data['date'].split('-')
        nc = _harmonize_hatpro_file(nc)
    if data['instrument'] == 'halo-doppler-lidar':
        nc.year, nc.month, nc.day = _get_halo_date(data)
        nc.renameVariable('height_asl', 'height')
    nc.history = _get_history(nc)
    nc.location = _get_location(nc, data)
    nc.title = _get_title(nc)
    if file_type in ('model', 'lidar'):  # HATPRO files contain multiple problems
        nc.Conventions = 'CF-1.7'
    nc.close()
    nc_raw.close()
    shutil.copy(temp_file.name, data['full_path'])
    return uuid


def copy_file_contents(source: netCDF4.Dataset,
                       target: netCDF4.Dataset,
                       time_ind: Optional[list] = None) -> None:
    """Copies netCDF contents. Optionally uses only certain time indices."""
    for key, dimension in source.dimensions.items():
        if key == 'time' and time_ind is not None:
            size = len(time_ind)
        else:
            size = dimension.size
        target.createDimension(key, size)
    for var_name, variable in source.variables.items():
        var_out = target.createVariable(var_name, variable.datatype, variable.dimensions, zlib=True)
        attr = {k: variable.getncattr(k) for k in variable.ncattrs()}
        if '_FillValue' in attr:
            del attr['_FillValue']
        var_out.setncatts(attr)
        if 'time' in variable.dimensions and time_ind is not None:
            array = variable[time_ind]
        else:
            array = variable[:]
        var_out[:] = array
    for attr_name in source.ncattrs():
        setattr(target, attr_name, source.getncattr(attr_name))


def _check_time_dimension(nc: netCDF4.Dataset, data: dict) -> None:
    n_steps = len(nc.dimensions['time'])
    n_steps_expected = 25
    n_steps_expected_gdas1 = 9
    if data['model'] == 'gdas1' and n_steps != n_steps_expected_gdas1:
        raise ValueError
    elif n_steps != n_steps_expected:
        raise ValueError


def _get_file_type(data: dict) -> str:
    if data['instrument'] is None:
        return 'model'
    return utils.get_level1b_type(data['instrument'])


def _get_valid_hatpro_timestamps(data: dict, nc: netCDF4.Dataset) -> list:
    time_stamps = nc.variables['time'][:]
    epoch = _get_epoch(nc.variables['time'].units)
    expected_date = data['date'].split('-')
    valid_ind = []
    for ind, t in enumerate(time_stamps):
        if seconds2date(t, epoch)[:3] == expected_date:
            valid_ind.append(ind)
    if not valid_ind:
        raise RuntimeError('All HATPRO dates differ from expected.')
    return valid_ind


def _get_halo_date(data: dict) -> tuple:
    original_filename = data['original_filename']
    year = f'{original_filename[:4]}'
    month = f'{original_filename[4:6]}'
    day = f'{original_filename[6:8]}'
    assert f'{year}-{month}-{day}' == data['date']
    return year, month, day


def _get_model_date(nc: netCDF4.Dataset) -> list:
    date_string = nc.variables['time'].units
    the_date = date_string.split()[2]
    return the_date.split('-')


def _harmonize_hatpro_file(nc: netCDF4.Dataset) -> netCDF4.Dataset:
    valid_name = 'LWP'
    for invalid_name in ('LWP_data', 'clwvi'):
        if invalid_name in nc.variables:
            nc.renameVariable(invalid_name, valid_name)
    if valid_name in nc.variables and 'kg' in nc.variables[valid_name].units:
        nc.variables[valid_name][:] *= 1000
        nc.variables[valid_name].units = 'g m-2'
    return nc


def _get_history(nc: netCDF4.Dataset) -> str:
    old_history = getattr(nc, 'history', '')
    new_record = f"{get_time()} - File content harmonized by the CLU unit.\n"
    return f"{new_record}{old_history}"


def _get_title(nc: netCDF4.Dataset) -> str:
    file_type = nc.cloudnet_file_type.capitalize()
    return f"{file_type} file from {nc.location.capitalize()}"


def _get_location(nc: netCDF4.Dataset, data: dict) -> str:
    if hasattr(nc, 'location') and len(nc.location) > 0:
        return nc.location
    return data['site_name'].capitalize()


def _get_epoch(units: str) -> tuple:
    fallback = (2001, 1, 1)
    try:
        date = units.split()[2]
    except IndexError:
        return fallback
    date = date.replace(',', '')
    try:
        date_components = [int(x) for x in date.split('-')]
    except ValueError:
        try:
            date_components = [int(x) for x in date.split('.')]
        except ValueError:
            return fallback
    year, month, day = date_components
    current_year = datetime.datetime.today().year
    if (1900 < year < current_year) and (0 < month < 13) and (0 < day < 32):
        return tuple(date_components)
    return fallback
