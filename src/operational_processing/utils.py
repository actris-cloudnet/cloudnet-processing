import os
import fnmatch
import requests
import datetime
import configparser
from cloudnetpy.utils import get_time


def read_site_info(site_name):
    """Read site information from Cloudnet http API."""
    url = f"https://altocumulus.fmi.fi/api/sites/"
    sites = requests.get(url=url).json()
    for site in sites:
        if site['id'] == site_name.replace('-', ''):
            site['id'] = site_name
            site['name'] = site.pop('humanReadableName')
            return site


def find_file(folder, wildcard):
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
            return '/'.join((folder, file))
    raise FileNotFoundError(f"No {wildcard} in {folder}")


def date_string_to_date(date_string):
    """Convert YYYY-MM-DD to Python date."""
    date = [int(x) for x in date_string.split('-')]
    return datetime.date(*date)


def get_date_from_past(n, reference_date=None):
    """Return date N-days ago."""
    reference = reference_date or get_time().split()[0]
    date = date_string_to_date(reference) - datetime.timedelta(n)
    return str(date)


def read_conf(args):
    conf_dir = args.config_dir
    def _read(conf_type):
        config_path = f'{conf_dir}/{conf_type}.ini'
        config = configparser.ConfigParser()
        config.readfp(open(config_path, 'r'))
        return config

    def _overwrite_path(name):
        if hasattr(args, name):
            value = getattr(args, name)
            if value:
                main_conf['PATH'][name] = value

    main_conf = _read('main')
    _overwrite_path('input')
    _overwrite_path('output')
    if 'site' in args:
        site_name = args.site[0]
        return {'main': main_conf,
                'site': _read(site_name)}
    return {'main': main_conf}

def str2bool(s):
    return False if s == 'False' else True if s == 'True' else s
