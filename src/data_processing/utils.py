import os
import fnmatch
import datetime
import configparser
import requests
from cloudnetpy.utils import get_time
from cloudnetpy.plotting.plot_meta import ATTRIBUTES as ATTR


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
            return os.path.join(folder, file)
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
        config.read_file(open(config_path, 'r'))
        return config

    def _overwrite_path(name):
        if hasattr(args, name):
            value = getattr(args, name)
            if value:
                main_conf['PATH'][name] = value

    main_conf = _read('main')
    _overwrite_path('input')
    _overwrite_path('output')
    if hasattr(args, 'site'):
        site_name = args.site[0]
        return {'main': main_conf,
                'site': _read(site_name)}
    return {'main': main_conf}


def str2bool(s):
    return False if s == 'False' else True if s == 'True' else s


def get_fields_for_plot(cloudnet_file_type):
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


def get_plottable_variables_info(cloudnet_file_type):
    """Find variable IDs and corresponding human readable names."""
    fields = get_fields_for_plot(cloudnet_file_type)[0]
    return {get_var_id(cloudnet_file_type, field): [f"{ATTR[field].name}", i]
            for i, field in enumerate(fields)}


def get_var_id(cloudnet_file_type, field):
    """Return identifier for variable / Cloudnet file combination."""
    return f"{cloudnet_file_type}-{field}"
