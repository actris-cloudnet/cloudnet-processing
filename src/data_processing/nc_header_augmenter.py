import os
from typing import Union
import netCDF4
from data_processing import utils
from cloudnetpy.utils import get_uuid, get_time


def fix_legacy_file(full_path: str,
                    temp_file: str) -> str:
    """Fix legacy netCDF file."""

    def _get_date():
        year = filename[:4]
        month = filename[4:6]
        day = filename[6:8]
        if int(nc.year) != int(year) or int(nc.month) != int(month) or int(nc.day) != int(day):
            nc.close()
            raise utils.MiscError('Not sure which date this is')
        return f'{year}-{month}-{day}'

    def _get_cloudnet_file_type():
        if 'iwc-Z-T-method' in filename:
            return 'iwc'
        if 'lwc-scaled-adiabatic' in filename:
            return 'lwc'
        for file_type in ('drizzle', 'classification', 'categorize'):
            if file_type in filename:
                return file_type
        nc.close()
        raise utils.MiscError('Undetected legacy file')

    uuid = get_uuid()
    filename = os.path.basename(full_path)
    nc = netCDF4.Dataset(full_path, 'a')
    date_str = _get_date()
    cloudnet_file_type = _get_cloudnet_file_type()
    history = _get_history(nc)

    nc_new = netCDF4.Dataset(temp_file, 'w', format='NETCDF4_CLASSIC')
    copy_file_contents(nc, nc_new)
    nc.close()

    # New / modified global attributes:
    nc_new.file_uuid = uuid
    nc_new.history = history
    nc_new.cloudnet_file_type = cloudnet_file_type
    nc_new.close()

    return date_str


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


def fix_mwr_file(full_path: str,
                 original_filename: str,
                 date_str: str,
                 site_name: str,
                 uuid: Union[str, bool, None]) -> str:
    """Fixes global attributes of raw MWR netCDF file."""

    def _get_date():
        year = f'20{original_filename[:2]}'
        month = f'{original_filename[2:4]}'
        day = f'{original_filename[4:6]}'
        assert f'{year}-{month}-{day}' == date_str
        return year, month, day

    nc = netCDF4.Dataset(full_path, 'a')
    uuid = uuid or get_uuid()
    nc.file_uuid = uuid
    nc.cloudnet_file_type = 'mwr'
    nc.history = _get_history(nc)
    nc.year, nc.month, nc.day = _get_date()
    nc.location = site_name
    nc.title = _get_title(nc)
    if not hasattr(nc, 'Conventions'):
        nc.Conventions = 'CF-1.0'
    nc.close()
    return uuid


def fix_model_file(full_path: str,
                   site_name: str,
                   uuid: Union[str, bool, None]) -> str:
    """Fixes global attributes of raw model netCDF file."""

    def _get_date():
        date_string = nc.variables['time'].units
        the_date = date_string.split()[2]
        return the_date.split('-')

    nc = netCDF4.Dataset(full_path, 'a')
    uuid = uuid or get_uuid()
    nc.file_uuid = uuid
    nc.cloudnet_file_type = 'model'
    nc.year, nc.month, nc.day = _get_date()
    nc.history = _get_history(nc)
    nc.title = _get_title(nc)
    nc.location = site_name
    nc.close()
    return uuid


def _get_history(nc: netCDF4.Dataset) -> str:
    old_history = getattr(nc, 'history', '')
    new_record = f"{get_time()} - File content harmonized by the CLU unit.\n"
    return f"{new_record}{old_history}"


def _get_title(nc: netCDF4.Dataset) -> str:
    return f"{nc.cloudnet_file_type.capitalize()} file from {nc.location.capitalize()}"
