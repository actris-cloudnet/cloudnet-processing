import os
import fnmatch
import requests
import datetime
from cloudnetpy.utils import get_time


def read_site_info(site_name):
    """Read site information from Cloudnet http API."""
    url = f"https://altocumulus.fmi.fi/api/sites/"
    sites = requests.get(url=url, verify=False).json()
    for site in sites:
        if site['id'] == site_name:
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


def get_date_from_past(n):
    """Return date N-days ago."""
    now = get_time().split()[0]
    now = date_string_to_date(now) - datetime.timedelta(n)
    return str(now)
