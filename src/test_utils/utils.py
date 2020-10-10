import os
import signal
import atexit
import shutil
import subprocess
import time
import socket


def kill_pid(name='data-processing'):
    for line in os.popen(f"ps ax | grep {name} | grep -v grep"):
        pid = line.split()[0]
        os.kill(int(pid), signal.SIGKILL)


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
    md_server = subprocess.Popen(['python3', '-u', 'src/test_utils/server.py', document_root, str(port)],
                                 stderr=logfile)
    atexit.register(md_server.terminate)
    wait_for_port(port)

    return md_server
