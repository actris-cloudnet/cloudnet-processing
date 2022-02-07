"""Helper functions."""
import base64
import datetime
import hashlib
import logging
import os
import random
import re
import shutil
import string
import sys
from typing import Tuple, Optional
from typing import Union
import netCDF4
import pytz
import requests
from cloudnetpy.plotting.plot_meta import ATTRIBUTES as ATTR
from cloudnetpy.plotting.plotting import Dimensions
from cloudnetpy_qc import Quality
from cloudnetpy.utils import get_time


def create_product_put_payload(full_path: str,
                               storage_service_response: dict,
                               product: Optional[str] = None,
                               site: Optional[str] = None,
                               date_str: Optional[str] = None) -> dict:
    """Creates put payload for data portal."""
    nc = netCDF4.Dataset(full_path, 'r')
    payload = {
        'product': product or nc.cloudnet_file_type,
        'site': site or nc.location.lower(),
        'measurementDate': date_str or f'{nc.year}-{nc.month}-{nc.day}',
        'format': get_file_format(nc),
        'checksum': sha256sum(full_path),
        'volatile': not hasattr(nc, 'pid'),
        'uuid': getattr(nc, 'file_uuid', ''),
        'pid': getattr(nc, 'pid', ''),
        'history': getattr(nc, 'history', ''),
        ** storage_service_response
    }
    source_uuids = getattr(nc, 'source_file_uuids', None)
    if source_uuids:
        payload['sourceFileIds'] = source_uuids.replace(' ', '').split(',')
    payload['cloudnetpyVersion'] = getattr(nc, 'cloudnetpy_version', '')
    nc.close()
    return payload


def get_file_format(nc: netCDF4.Dataset):
    """Returns netCDF file format."""
    file_format = nc.file_format.lower()
    if 'netcdf4' in file_format:
        return 'HDF5 (NetCDF4)'
    if 'netcdf3' in file_format:
        return 'NetCDF3'
    raise RuntimeError('Unknown file type')


def read_site_info(site_name: str) -> dict:
    """Reads site information from Cloudnet http API."""
    sites = get_from_data_portal_api('api/sites', {'developer': True})
    for site in sites:
        if site['id'] == site_name:
            site['id'] = site_name
            site['name'] = site.pop('humanReadableName')
            return site


def get_product_types(level: Optional[str] = None) -> list:
    """Returns Cloudnet processing types."""
    products = get_from_data_portal_api('api/products', {'developer': True})
    if level is not None:
        return [product['id'] for product in products if product['level'] == level]
    return [product['id'] for product in products]


def get_calibration_factor(site: str, date: str, instrument: str) -> Union[float, None]:
    """Gets calibration factor."""
    data_portal_url = fetch_data_portal_url()
    url = f"{data_portal_url}api/calibration"
    payload = {
        'site': site,
        'date': date,
        'instrument': instrument
    }
    res = requests.get(url, payload)
    if not res.ok:
        return None
    return res.json()[0].get('calibrationFactor', None)


def get_model_types() -> list:
    """Returns list of model types."""
    models = get_from_data_portal_api('api/models')
    return [model['id'] for model in models]


def date_string_to_date(date_string: Union[str, None]) -> Union[datetime.date, None]:
    """Convert YYYY-MM-DD to Python date."""
    if date_string is None:
        return None
    date = [int(x) for x in date_string.split('-')]
    return datetime.date(*date)


def get_date_from_past(n: int, reference_date: Optional[str] = None) -> str:
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


def send_slack_alert(error_msg,
                     error_source: str,
                     site: Optional[str] = None,
                     date: Optional[str] = None,
                     product: Optional[str] = None,
                     critical: Optional[bool] = False) -> None:
    """Sends notification to slack."""
    config = read_main_conf()
    if critical is True:
        logging.critical(error_msg)
    else:
        logging.error(error_msg)

    key = 'SLACK_API_TOKEN'
    if key not in config or config[key] == '':
        return

    if error_source == 'model':
        label = ':earth_africa: Model processing'
    elif error_source == 'pid':
        label = ':id: PID generation'
    elif error_source == 'data':
        label = ':desktop_computer: Data processing'
    elif error_source == 'wrapper':
        label = ':fire: Main wrapper'
    elif error_source == 'img':
        label = ':frame_with_picture: Image creation'
    else:
        raise ValueError('Unknown error source')

    with open('all.log') as file:
        log = file.readlines()

    padding = ' '*7
    msg = f'*{label}*\n\n'

    for name, var in zip(('Site', 'Date', 'Product'), (site, date, product)):
        if var is not None:
            msg += f'*{name}:* {var}{padding}'

    timestamp = str(get_helsinki_datetime())[:19]
    msg += f'*Time:* {timestamp}\n\n'
    msg += f'*Error:* {error_msg}'

    payload = {'content': ''.join(log),
               'channels': 'C022YBMQ2KC',
               'title': 'Full log',
               'initial_comment': msg}

    requests.post('https://slack.com/api/files.upload',
                  data=payload,
                  headers={"Authorization": f'Bearer {config[key]}'})


def read_main_conf() -> dict:
    """Reads config from env vars."""
    return dict(os.environ)


def str2bool(s: str) -> Union[bool, str]:
    """Converts string to bool."""
    return False if s == 'False' else True if s == 'True' else s


def get_plottable_variables_info(cloudnet_file_type: str) -> dict:
    """Find variable IDs and corresponding human readable names."""
    fields = get_fields_for_plot(cloudnet_file_type)[0]
    return {get_var_id(cloudnet_file_type, field): [f"{ATTR[field].name}", i]
            for i, field in enumerate(fields)}


def get_fields_for_plot(cloudnet_file_type: str) -> Tuple[list, int]:
    """Return list of variables and maximum altitude for Cloudnet quicklooks.

    Args:
        cloudnet_file_type (str): Name of Cloudnet file type, e.g., 'classification'.

    Returns:
        tuple: 2-element tuple containing feasible variables for plots
        (list) and maximum altitude (int).

    """
    max_alt = 12
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
        fields = ['beta', 'beta_raw', 'depolarisation', 'depolarisation_raw']
    elif cloudnet_file_type == 'mwr':
        fields = ['lwp']
    elif cloudnet_file_type == 'radar':
        fields = ['Zh', 'v', 'width', 'ldr', 'sldr']
    elif cloudnet_file_type == 'disdrometer':
        fields = ['rainfall_rate', 'n_particles']
    elif cloudnet_file_type == 'drizzle':
        fields = ['Do', 'drizzle_N']
        max_alt = 4
    else:
        raise NotImplementedError
    return fields, max_alt


def get_fields_for_l3_plot(product: str, model: str) -> list:
    """Return list of variables and maximum altitude for Cloudnet quicklooks.

    Args:
        product (str): Name of product, e.g., 'iwc'.
        model (str): Name of the model, e.g., 'ecmwf'.
    Returns:
        list: List of wanted variables
    """
    if product == 'l3-iwc':
        fields = [f'{model}_iwc', f'iwc_{model}']
    elif product == 'l3-lwc':
        fields = [f'{model}_lwc', f'lwc_{model}']
    elif product == 'l3-cf':
        fields = [f'{model}_cf', f'cf_V_{model}']
    else:
        raise NotImplementedError
    return fields


def get_var_id(cloudnet_file_type: str, field: str) -> str:
    """Return identifier for variable / Cloudnet file combination."""
    return f"{cloudnet_file_type}-{field}"


def sha256sum(filename: str) -> str:
    """Calculates hash of file using sha-256."""
    return _calc_hash_sum(filename, 'sha256')


def md5sum(filename: str, is_base64: Optional[bool] = False) -> str:
    """Calculates hash of file using md5."""
    return _calc_hash_sum(filename, 'md5', is_base64)


def _calc_hash_sum(filename, method, is_base64: Optional[bool] = False) -> str:
    hash_sum = getattr(hashlib, method)()
    with open(filename, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_sum.update(byte_block)
    if is_base64:
        return base64.encodebytes(hash_sum.digest()).decode('utf-8').strip()
    return hash_sum.hexdigest()


def get_product_bucket(volatile: Optional[bool] = False) -> str:
    """Retrurns correct s3 bucket."""
    return 'cloudnet-product-volatile' if volatile else 'cloudnet-product'


def is_volatile_file(filename: str) -> bool:
    """Check if nc-file is volatile."""
    nc = netCDF4.Dataset(filename)
    is_missing_pid = not hasattr(nc, 'pid')
    nc.close()
    return is_missing_pid


def get_product_identifier(product: str) -> str:
    """Returns product identifier."""
    if product == 'iwc':
        return 'iwc-Z-T-method'
    if product == 'lwc':
        return 'lwc-scaled-adiabatic'
    return product


def get_model_identifier(filename: str) -> str:
    """Returns model identifier."""
    return filename.split('_')[-1][:-3]


def get_level1b_type(instrument_id: str) -> str:
    """Returns level 1b types."""
    data = get_from_data_portal_api('api/instruments')
    return [instru['type'] for instru in data if instrument_id == instru['id']][0]


class MiscError(Exception):
    """Internal exception class."""
    def __init__(self, msg: str):
        self.message = msg
        super().__init__(self.message)


class RawDataMissingError(Exception):
    """Internal exception class."""
    def __init__(self, msg: Optional[str] = 'Missing raw data'):
        self.message = msg
        super().__init__(self.message)


class SkipBlock(Exception):
    """Internal exception class."""
    def __init__(self, msg: Optional[str] = ''):
        self.message = msg
        super().__init__(self.message)


def datetime_to_utc(date_time: str, time_zone_name: str) -> str:
    """Converts local datetime at some time zone to UTC."""
    time_zone = pytz.timezone(time_zone_name)
    utc_timezone = pytz.timezone('UTC')
    dt = datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
    dt = time_zone.localize(dt)
    dt = dt.astimezone(utc_timezone)
    dt = dt.strftime("%Y-%m-%d %H:%M:%S")
    return dt


def shift_datetime(date_time: str, offset: int) -> str:
    """Shifts datetime N hours."""
    dt = datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
    dt = dt + datetime.timedelta(hours=offset)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def get_helsinki_datetime() -> datetime.datetime:
    """Returns Helsinki datetime in UTC."""
    time_zone = pytz.timezone('Europe/Helsinki')
    dt = datetime.datetime.now()
    return dt.astimezone(time_zone)


def concatenate_text_files(filenames: list, output_filename: str) -> None:
    """Concatenates text files."""
    with open(output_filename, 'wb') as target:
        for filename in filenames:
            with open(filename, 'rb') as source:
                shutil.copyfileobj(source, target)


def init_logger(args: Optional = None) -> None:
    """Initializes logger."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    output_file_handler = logging.FileHandler("all.log", mode='w')
    output_file_handler.setFormatter(formatter)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(formatter)
    logger.addHandler(output_file_handler)
    logger.addHandler(stderr_handler)
    script_name = args.cmd
    msg = f'Starting {script_name}'
    msg += f' with args {vars(args)}' if args is not None else ''
    logging.info(msg)


def create_quality_report(filename: str) -> dict:
    """Creates quality report for data portal."""
    quality = Quality(filename)
    meta_res = quality.check_metadata()
    data_res = quality.check_data()
    quality.close()
    return {
        'checkedAt': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'overallScore': _calc_quality(quality),
        'metadata': _parse_quality_result(meta_res),
        'data': _parse_quality_result(data_res)}


def _parse_quality_result(quality_result: dict) -> list:
    test_array = []
    for test_name, value in quality_result.items():
        errors = []
        if test_name in ('outOfBounds', 'invalidGlobalAttributeValues'):
            for name, detected, limits in value:
                prefix = 'variable' if 'Bounds' in test_name else 'attribute'
                diagnostics = {
                    f'{prefix}Name': name,
                    'lowerBound': float(limits[0]),
                    'upperBound': float(limits[1]),
                }
                exceeding_key = 'exceedingValue'
                if test_name == 'outOfBounds':
                    min_value, max_value = [round(float(x), 4) for x in detected]
                    if min_value < limits[0]:
                        diagnostics[exceeding_key] = min_value
                    if max_value > limits[1]:
                        diagnostics[exceeding_key] = max_value
                else:
                    if detected < limits[0] or detected > limits[1]:
                        diagnostics[exceeding_key] = detected
                errors.append(diagnostics)
        elif test_name == 'invalidUnits':
            for name, unit, expected_unit in value:
                errors.append({
                    'variableName': name,
                    'expectedUnit': expected_unit,
                    'unit': unit,
                })
        else:
            errors = value
        test_array.append({
            'name': _format_test_name(test_name),
            'report': errors,
            'description': _get_test_description(test_name)
        })
    return test_array


def _format_test_name(test_name: str) -> str:
    """Changes 'outOfBounds' to 'Out of bounds', etc."""
    capitalized_indices = [ind for ind, c in enumerate(test_name) if c.isupper()]
    capitalized_indices.reverse()
    chars = [test_name[ind] for ind in capitalized_indices]
    for ind, char in zip(capitalized_indices, chars):
        test_name = test_name[:ind] + f' {char.lower()}' + test_name[ind + 1:]
    return test_name.capitalize()


def _get_test_description(test_name: str) -> str:
    if test_name == 'outOfBounds':
        return 'Find data values that are unexpectedly large or small.'
    if test_name == 'invalidGlobalAttributeValues':
        return 'Find attribute values that are unexpectedly large or small.'
    if test_name == 'invalidUnits':
        return 'Find unexpected variable units.'
    if test_name == 'missingVariables':
        return 'Find variables that should be included.'
    if test_name == 'missingGlobalAttributes':
        return 'Find global attributes that should be included.'
    return ''


def _calc_quality(quality: Quality) -> float:
    n_tests = quality.n_metadata_tests + quality.n_data_tests
    n_failures = quality.n_metadata_test_failures + quality.n_data_test_failures
    return round(1 - (n_failures / n_tests), 2)


def get_temp_dir(config: dict) -> str:
    """Returns temporary directory path."""
    return config.get('TEMP_DIR', '/tmp')


def get_cloudnet_sites() -> list:
    """Returns cloudnet site identifiers."""
    sites = get_from_data_portal_api('api/sites')
    sites = [site['id'] for site in sites if 'cloudnet' in site['type']]
    return sites


def get_from_data_portal_api(end_point: str, payload: Optional[dict] = None) -> list:
    """Reads from data portal API."""
    data_portal_url = fetch_data_portal_url()
    url = f'{data_portal_url}{end_point}'
    return requests.get(url=url, params=payload).json()


def fetch_data_portal_url() -> str:
    """Returns data portal url."""
    config = read_main_conf()
    return config['DATAPORTAL_URL']


def random_string(n: Optional[int] = 10) -> str:
    """Creates random string."""
    return ''.join(random.choices(string.ascii_lowercase, k=n))


def full_product_to_l3_product(full_product: str):
    """Returns l3 product name."""
    return full_product.split('-')[1]


def order_metadata(metadata: list) -> list:
    """Orders 2-element metadata according to measurementDate."""
    key = 'measurementDate'
    if len(metadata) == 2 and metadata[0][key] > metadata[1][key]:
        metadata.reverse()
    return metadata


def get_valid_uuids(uuids: list, full_paths: list, valid_full_paths: list) -> list:
    """Returns valid uuids."""
    return [uuid for uuid, full_path in zip(uuids, full_paths) if full_path in valid_full_paths]


def include_records_with_pattern_in_filename(metadata: list, pattern: str) -> list:
    """Includes only records with certain pattern."""
    return [row for row in metadata if re.search(pattern.lower(), row['filename'].lower())]


def exclude_records_with_pattern_in_filename(metadata: list, pattern: str) -> list:
    """Excludes records with certain pattern."""
    return [row for row in metadata if not re.search(pattern.lower(), row['filename'].lower())]


def get_processing_dates(args):
    """Returns processing dates."""
    if args.date is not None:
        start_date = args.date
        stop_date = get_date_from_past(-1, start_date)
    else:
        start_date = args.start
        stop_date = args.stop
    start_date = date_string_to_date(start_date)
    stop_date = date_string_to_date(stop_date)
    return start_date, stop_date


def dimensions2dict(dimensions: Dimensions) -> dict:
    """Converts dimensions object to dictionary."""
    return {
        'width': dimensions.width,
        'height': dimensions.height,
        'marginTop': dimensions.margin_top,
        'marginLeft': dimensions.margin_left,
        'marginBottom': dimensions.margin_bottom,
        'marginRight': dimensions.margin_right
    }


def check_chm_version(filename: str, expected_version: str):
    nc = netCDF4.Dataset(filename)
    source = getattr(nc, 'source', '')[:3].lower()
    nc.close()
    if (expected_version == 'chm15x' and source != 'chx') or (expected_version == 'chm15k' and source != 'chm'):
        logging.warning(f'Data submitted with incorrect instrument id')
