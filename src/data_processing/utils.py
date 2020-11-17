import os
from os import path
import fnmatch
import datetime
import configparser
import hashlib
import requests
from typing import Tuple, Union
import netCDF4
from cloudnetpy.utils import get_time
from cloudnetpy.plotting.plot_meta import ATTRIBUTES as ATTR


def read_site_info(site_name: str) -> dict:
    """Read site information from Cloudnet http API."""
    url = f"https://cloudnet.fmi.fi/api/sites?developer"
    sites = requests.get(url=url).json()
    for site in sites:
        if site['id'] == site_name.replace('-', ''):
            site['id'] = site_name
            site['name'] = site.pop('humanReadableName')
            return site


def find_file(folder: str, wildcard: str) -> str:
    """Find the first file name matching a wildcard.

    Args:
        folder (str): Name of folder.
        wildcard (str): pattern to be searched, e.g., '*some_string*'.

    Returns:
        str: Full path of the first found file.

    Raises:
        FileNotFoundError: Can not find such file.

    """
    files = os.listdir(folder)
    for file in files:
        if fnmatch.fnmatch(file, wildcard):
            return os.path.join(folder, file)
    raise FileNotFoundError(f"No {wildcard} in {folder}")


def list_files(folder: str, pattern: str) -> list:
    """List files from folder (non-recursively) using a pattern that can include wildcard.
    If folder or suitable files do not exist, return empty list."""
    if os.path.isdir(folder):
        files = fnmatch.filter(os.listdir(folder), pattern)
        return [path.join(folder, file) for file in files]
    return []


def date_string_to_date(date_string: str) -> datetime.date:
    """Convert YYYY-MM-DD to Python date."""
    date = [int(x) for x in date_string.split('-')]
    return datetime.date(*date)


def get_date_from_past(n: int, reference_date: str = None) -> str:
    """Return date N-days ago.

    Args:
        n (int): Number of days to skip (can be negative, when it means the future).
        reference_date (str, optional): Date as "YYYY-MM-DD". Default is the current date.

    Returns:
        str: Date as "YYYY-MM-DD".

    """
    reference = reference_date or get_time().split()[0]
    date = date_string_to_date(reference) - datetime.timedelta(n)
    return str(date)


def read_main_conf(args):
    """Read data-processing main config-file."""
    config_path = f'{args.config_dir}/main.ini'
    config = configparser.ConfigParser()
    config.read_file(open(config_path, 'r'))
    return config


def str2bool(s: str) -> Union[bool, str]:
    return False if s == 'False' else True if s == 'True' else s


def get_fields_for_plot(cloudnet_file_type: str) -> Tuple[list, int]:
    """Return list of variables and maximum altitude for Cloudnet quicklooks.

    Args:
        cloudnet_file_type (str): Name of Cloudnet file type, e.g., 'classification'.

    Returns:
        tuple: 2-element tuple containing feasible variables for plots
        (list) and maximum altitude (int).

    """
    max_alt = 10
    if cloudnet_file_type == 'categorize':
        fields = ['Z', 'v', 'width', 'ldr', 'v_sigma', 'beta', 'lwp', 'Tw',
                  'radar_gas_atten', 'radar_liquid_atten']
    elif cloudnet_file_type == 'classification':
        fields = ['target_classification', 'detection_status']
    elif cloudnet_file_type == 'iwc':
        fields = ['iwc', 'iwc_error', 'iwc_retrieval_status']
    elif cloudnet_file_type == 'lwc':
        fields = ['lwc', 'lwc_error', 'lwc_retrieval_status']
        max_alt = 6
    elif cloudnet_file_type == 'model':
        fields = ['cloud_fraction', 'uwind', 'vwind', 'temperature', 'q', 'pressure']
    elif cloudnet_file_type == 'lidar':
        fields = ['beta', 'beta_raw']
    elif cloudnet_file_type == 'radar':
        fields = ['Ze', 'v', 'width', 'ldr']
    elif cloudnet_file_type == 'drizzle':
        fields = ['Do', 'mu', 'S', 'drizzle_N', 'drizzle_lwc', 'drizzle_lwf', 'v_drizzle', 'v_air']
        max_alt = 4
    else:
        raise NotImplementedError
    return fields, max_alt


def get_plottable_variables_info(cloudnet_file_type: str) -> dict:
    """Find variable IDs and corresponding human readable names."""
    fields = get_fields_for_plot(cloudnet_file_type)[0]
    return {get_var_id(cloudnet_file_type, field): [f"{ATTR[field].name}", i]
            for i, field in enumerate(fields)}


def get_var_id(cloudnet_file_type: str, field: str) -> str:
    """Return identifier for variable / Cloudnet file combination."""
    return f"{cloudnet_file_type}-{field}"


def sha256sum(filename: str) -> str:
    """Calculates hash of file using sha-256."""
    hash_sum = hashlib.sha256()
    with open(filename, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_sum.update(byte_block)
    return hash_sum.hexdigest()


def add_hash_to_filename(filename: str, hash_sum: str) -> str:
    hash_to_name = hash_sum[:18]
    parts = filename.split('.')
    if len(parts) == 1:
        return f"{filename}-{hash_to_name}"
    return f"{''.join(parts[:-1])}-{hash_to_name}.{parts[-1]}"


def add_uuid_to_filename(uuid: str, filename: str) -> str:
    """Adds uuid suffix to file."""
    suffix = f"_{uuid[:4]}"
    filepath, extension = os.path.splitext(filename)
    new_filename = f"{filepath}{suffix}{extension}"
    os.rename(filename, new_filename)
    return new_filename


def is_volatile_file(filename: str) -> bool:
    """Check if nc-file is volatile."""
    nc = netCDF4.Dataset(filename)
    is_missing_pid = not hasattr(nc, 'pid')
    nc.close()
    return is_missing_pid


def replace_path(filename: str, new_path: str) -> str:
    """Replaces path of file."""
    return filename.replace(os.path.dirname(filename), new_path)
