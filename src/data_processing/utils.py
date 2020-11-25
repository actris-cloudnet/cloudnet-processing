import datetime
import configparser
import hashlib
import requests
from typing import Tuple, Union
from cloudnetpy.utils import get_time
from cloudnetpy.plotting.plot_meta import ATTRIBUTES as ATTR
import base64


def read_site_info(site_name: str) -> dict:
    """Read site information from Cloudnet http API."""
    url = f"https://cloudnet.fmi.fi/api/sites?modelSites"
    sites = requests.get(url=url).json()
    for site in sites:
        if site['id'] == site_name.replace('-', ''):
            site['id'] = site_name
            site['name'] = site.pop('humanReadableName')
            return site


def get_raw_processing_types() -> list:
    """Return Cloudnet raw file processing types."""
    url = f"https://cloudnet.fmi.fi/api/instruments"
    instruments = requests.get(url=url).json()
    all_types = [instrument['type'] for instrument in instruments]
    all_types.append('model')
    return list(set(all_types))


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
    return _calc_hash_sum(filename, 'sha256')


def md5sum(filename: str, is_base64=False) -> str:
    """Calculates hash of file using md5."""
    return _calc_hash_sum(filename, 'md5', is_base64)


def _calc_hash_sum(filename, method, is_base64=False):
    hash_sum = getattr(hashlib, method)()
    with open(filename, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_sum.update(byte_block)
    if is_base64:
        return base64.encodebytes(hash_sum.digest()).decode('utf-8').strip()
    else:
        return hash_sum.hexdigest()


def get_product_bucket(volatile: bool = False) -> str:
    return 'cloudnet-product-volatile' if volatile else 'cloudnet-product'
