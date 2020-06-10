import netCDF4
from cloudnetpy.utils import get_uuid, get_time

MAJOR = 0
MINOR = 1
PATCH = 0
VERSION = '%d.%d.%d' % (MAJOR, MINOR, PATCH)


def fix_attributes(file_name, overwrite=False):
    """Fixes global attributes of a netCDF file."""
    nc = netCDF4.Dataset(file_name, 'a')
    if hasattr(nc, 'file_uuid') and not overwrite:
        nc.close()
        return None
    uuid = get_uuid()
    nc.file_uuid = uuid
    nc.cloudnet_file_type = _get_file_type(nc)
    nc.history = _add_history(nc)
    try:
        nc.year, nc.month, nc.day = _get_date(nc)
    except ValueError:
        pass
    try:
        nc.title = _get_title(nc)
    except AttributeError:
        pass
    nc.close()
    return uuid


def _get_file_type(nc):
    source = getattr(nc, 'source', '').lower()
    if any(model in source for model in ('ecmwf', 'icon')):
        return 'model'
    elif hasattr(nc, 'radiometer_system'):
        return 'mwr'
    return ''


def _add_history(nc):
    old_history = getattr(nc, 'history', '')
    new_record = f"{get_time()} - global attributes fixed using attribute_modifier {VERSION}\n"
    return f"{new_record}{old_history}"


def _get_date(nc):
    date_string = nc.variables['time'].units
    the_date = date_string.split()[2]
    return the_date.split('-')


def _get_title(nc):
    return f"{nc.cloudnet_file_type.capitalize()} file from {nc.location}"
