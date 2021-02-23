import shutil
from tempfile import NamedTemporaryFile
import netCDF4
from cloudnetpy.utils import get_uuid, get_time, seconds2date
from data_processing import utils


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
    copy_file_contents(nc_raw, nc)
    uuid = data['uuid'] or get_uuid()
    nc.file_uuid = uuid
    file_type = _get_file_type(data)
    nc.cloudnet_file_type = file_type
    if file_type == 'model':
        nc.year, nc.month, nc.day = _get_model_date(nc)
    if data['instrument'] == 'hatpro':
        nc.year, nc.month, nc.day = _get_hatpro_date(data, nc_raw)
        if 'LWP_data' in nc_raw.variables:
            nc.renameVariable('LWP_data', 'LWP')
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


def _get_file_type(data: dict) -> str:
    if data['instrument'] is None:
        return 'model'
    return utils.get_level1b_type(data['instrument'])


def _get_hatpro_date(data: dict, nc: netCDF4.Dataset) -> tuple:
    time_stamps = nc.variables['time'][:]
    # Some of the last timestamps might go to next day. Deal with this later.
    for t in time_stamps[:-10]:
        if seconds2date(t)[:3] != data['date'].split('-'):
            raise RuntimeError('Bad HATPRO dates.')
    return seconds2date(time_stamps[0])[:3]


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


def copy_file_contents(source: netCDF4.Dataset, target: netCDF4.Dataset) -> None:
    for key, dimension in source.dimensions.items():
        target.createDimension(key, dimension.size)
    for var_name, variable in source.variables.items():
        var_out = target.createVariable(var_name, variable.datatype, variable.dimensions,
                                        zlib=True)
        attr = {k: variable.getncattr(k) for k in variable.ncattrs()}
        if '_FillValue' in attr:
            del attr['_FillValue']
        var_out.setncatts(attr)
        var_out[:] = variable[:]
    for attr_name in source.ncattrs():
        setattr(target, attr_name, source.getncattr(attr_name))


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
