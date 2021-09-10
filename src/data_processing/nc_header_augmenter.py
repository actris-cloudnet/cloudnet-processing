import shutil
from typing import Optional
from tempfile import NamedTemporaryFile
import cloudnetpy.utils
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
        nc = _harmonize_hatpro_file(nc, data)
    if data['instrument'] == 'halo-doppler-lidar':
        nc.year, nc.month, nc.day = data['date'].split('-')
        nc.renameVariable('range', 'height')
        nc.variables['height'][:] += nc.variables['altitude']
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
        dtype = 'double' if var_name == 'time' else variable.dtype
        var_out = target.createVariable(var_name, dtype, variable.dimensions, zlib=True)
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
    if data['model'] == 'gdas1' and n_steps == n_steps_expected_gdas1:
        return
    if data['model'] != 'gdas1' and n_steps == n_steps_expected:
        return
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
        if (0 < t < 24 and epoch == expected_date) or (seconds2date(t, epoch)[:3] == expected_date):
            valid_ind.append(ind)
    if not valid_ind:
        raise RuntimeError('All HATPRO dates differ from expected.')
    return valid_ind


def _get_model_date(nc: netCDF4.Dataset) -> list:
    date_string = nc.variables['time'].units
    the_date = date_string.split()[2]
    return the_date.split('-')


def _harmonize_hatpro_file(nc: netCDF4.Dataset, data: dict) -> netCDF4.Dataset:
    valid_name = 'LWP'
    valid_unit = 'g m-2'
    for invalid_name in ('LWP_data', 'clwvi', 'atmosphere_liquid_water_content'):
        if invalid_name in nc.variables:
            nc.renameVariable(invalid_name, valid_name)
    assert valid_name in nc.variables
    if 'kg' in nc.variables[valid_name].units:
        nc.variables[valid_name][:] *= 1000
    nc.variables[valid_name].units = valid_unit
    nc = _sort_time(nc, valid_name)
    nc = _convert_hatpro_time(nc, data)
    _check_time_reference(nc)
    nc = _add_altitude(nc, data)
    return nc


def _add_altitude(nc: netCDF4.Dataset, data: dict) -> netCDF4.Dataset:
    key = 'altitude'
    alt = nc.createVariable(key, 'i4') if key not in nc.variables else nc.variables[key]
    alt[:] = data[key]
    alt.units = 'm'
    alt.long_name = 'Altitude of site'
    return nc


def _convert_hatpro_time(nc: netCDF4.Dataset, data: dict):
    key = 'time'
    time = nc.variables[key]
    if max(time[:]) > 24:
        fraction_hour = cloudnetpy.utils.seconds2hours(time[:])
        nc.variables[key][:] = fraction_hour
    nc.variables[key].long_name = 'Time UTC'
    nc.variables[key].units = f'hours since {data["date"]} 00:00:00'
    return nc


def _check_time_reference(nc: netCDF4.Dataset):
    key = 'time_reference'
    if key in nc.variables:
        assert nc.variables[key][:] == 1  # 1 = UTC. This check is for Palaiseau HATPRO files.


def _sort_time(nc: netCDF4.Dataset, key: str) -> netCDF4.Dataset:
    time = nc.variables['time'][:]
    array = nc.variables[key][:]
    ind = time.argsort()
    nc.variables['time'][:] = time[ind]
    nc.variables[key][:] = array[ind]
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
    if (1900 < year <= current_year) and (0 < month < 13) and (0 < day < 32):
        return tuple(date_components)
    return fallback
