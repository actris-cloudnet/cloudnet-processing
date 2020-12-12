from typing import Union
import netCDF4
from cloudnetpy.utils import get_uuid, get_time


def fix_legacy_file(full_path: str,
                    temp_file: str) -> str:
    """Fix legacy netCDF file."""

    uuid = get_uuid()

    nc = netCDF4.Dataset(full_path, 'r')
    nc_new = netCDF4.Dataset(temp_file, 'w', format='NETCDF4_CLASSIC')

    copy_file_contents(nc, nc_new)
    history = _get_history(nc)

    nc_new.file_uuid = uuid
    nc_new.history = history

    nc.close()
    nc_new.close()

    return uuid


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
