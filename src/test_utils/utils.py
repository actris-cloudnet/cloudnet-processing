from tempfile import NamedTemporaryFile
import atexit
import os
import re
import io
import sys
import shutil
import subprocess
import time
import socket
import requests
import requests_mock
from data_processing.utils import get_product_types
sys.path.append('scripts/')
PROCESS_CLOUDNET = __import__("process-cloudnet")
PROCESS_CLOUDNET_MODEL = __import__("process-model")


def init_test_session():
    adapter = requests_mock.Adapter()
    session = requests.Session()
    session.mount('http://', adapter)
    mock_addr = 'http://test/'
    return session, adapter, mock_addr


def start_output_capturing():
    old_stderr = sys.stderr
    stderr = io.StringIO()
    sys.stderr = stderr
    return old_stderr, stderr


def reset_output(old_stderr, stderr):
    output = stderr.getvalue()
    sys.stderr = old_stderr
    return output


def wait_for_port(port, host='localhost', timeout=10.0):
    """Wait until a port starts accepting TCP connections. Used in e2e-tests.
    Args:
        port (int): Port number.
        host (str): Host address on which the port should exist.
        timeout (float): In seconds. How long to wait before raising errors.
    Raises:
        TimeoutError: The port isn't accepting connection after time specified in `timeout`.
    """
    start_time = time.perf_counter()
    while True:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                break
        except OSError as ex:
            time.sleep(0.01)
            if time.perf_counter() - start_time >= timeout:
                raise TimeoutError('Waited too long for the port {} on host {} to start accepting '
                                   'connections.'.format(port, host)) from ex


def remove_dir(target):
    try:
        shutil.rmtree(target)
    except FileNotFoundError:
        pass


def remove_dirs(target, keep=()):
    for item in os.listdir(target):
        if item not in keep:
            shutil.rmtree('/'.join((target, item)))


def remove_files(target):
    for file in os.listdir(target):
        full_path = '/'.join((target, file))
        if os.path.isfile(full_path):
            os.remove(full_path)


def copy_files(source, target):
    for file in os.listdir(source):
        full_src_path = '/'.join((source, file))
        full_trg_path = '/'.join((target, file))
        if os.path.isfile(full_src_path):
            shutil.copy(full_src_path, full_trg_path)


def start_server(port, document_root, log_path):
    logfile = open(log_path, 'w')
    md_server = subprocess.Popen(['python3', '-u', 'src/test_utils/server.py', document_root,
                                  str(port)], stderr=logfile)
    atexit.register(md_server.terminate)
    wait_for_port(port)

    return md_server


def count_strings(data: list, string: str) -> int:
    n = 0
    for row in data:
        if string in row:
            n += 1
    return n


def register_storage_urls(temp_file: NamedTemporaryFile,
                          source_data: list,
                          site: str,
                          date: str,
                          identifier: str,
                          is_volatile: bool,
                          products=None):
    def save_file(request):
        with open(temp_file.name, mode='wb') as file:
            file.write(request.body.read())
        return True

    session, adapter, mock_addr = init_test_session()
    source_dir, end_point, is_level_2_product = _get_source_file_paths(identifier)
    for uuid, filename in source_data:
        prefix = '' if is_level_2_product else f'/{site}/{uuid}'
        url = f'{mock_addr}{end_point}{prefix}/{filename}'
        adapter.register_uri('GET', url, body=open(f'{source_dir}/{filename}', 'rb'))
    bucket_suffix = '-volatile' if is_volatile is True else ''
    date_stripped = date.replace('-', '')
    if products is None:
        products = (_fix_identifier(identifier),)
    for product in products:
        url = f'{mock_addr}cloudnet-product{bucket_suffix}/{date_stripped}_{site}_{product}.nc'
        adapter.register_uri('PUT', url, additional_matcher=save_file, json={'size': 65,
                                                                             'version': ''})
    adapter.register_uri('PUT', re.compile(f'{mock_addr}cloudnet-img/(.*?)'))
    return session


def _fix_identifier(identifier: str) -> str:
    for n in range(10):
        identifier = identifier.replace(f'_{n}', '')
    return identifier


def _get_source_file_paths(identifier: str) -> tuple:
    is_level_2_product = identifier in get_product_types('2') or identifier == 'categorize'
    if is_level_2_product is True:
        source_dir = 'tests/data/products'
        end_point = 'cloudnet-product'
    else:
        source_dir = f'tests/data/raw/{identifier}'
        end_point = 'cloudnet-upload'
    return source_dir, end_point, is_level_2_product


def start_test_servers(instrument: str, script_path: str):
    dir_name = 'tests/data/server/'
    start_server(5000, f'{dir_name}metadata/process_{instrument}', f'{script_path}/md.log')
    start_server(5001, f'{dir_name}pid', f'{script_path}/pid.log')


def process(session,
            main_args: list,
            temp_file: NamedTemporaryFile,
            script_path: str,
            marker: str = None,
            is_model_processing: bool = False):
    if is_model_processing is True:
        PROCESS_CLOUDNET_MODEL.main(main_args, storage_session=session)
    else:
        PROCESS_CLOUDNET.main(main_args, storage_session=session)
    pytest_args = ['pytest', '-v', '-s', f'{script_path}/tests.py', '--full_path', temp_file.name,
                   '--args', str(main_args)]
    try:
        if marker is not None:
            pytest_args += ['-m', marker]
        subprocess.check_call(pytest_args)
    except subprocess.CalledProcessError:
        raise


def reset_log_file(script_path: str):
    open(f'{script_path}/md.log', 'w').close()


def parse_args(args: str) -> list:
    chars_to_remove = ['\'', '"', '[', ']', '-d=', '-p=', '--date=', '--product=', ' ']
    for c in chars_to_remove:
        args = args.replace(c, '')
    return args.split(',')
