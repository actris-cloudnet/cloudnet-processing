#!/usr/bin/env python3
import subprocess
from os import path
import time
import requests
from test_utils.utils import start_server, remove_dir, kill_pid
import signal

SCRIPT_PATH = path.dirname(path.realpath(__file__))

file = {'name': '20200405_granada_ecmwf.nc',
        'hashSum': '8ea4732a1234f66c374a518f2be041c15087139341103c9c3cbf8680de507424',
        'measurementDate': '2020-04-05',
        'modelType': 'ecmwf',
        'site': 'granada'}


def main():
    api_files_dir = 'tests/data/api_files/'
    remove_dir(api_files_dir)
    web_server = subprocess.Popen(['python3', 'scripts/run-data-submission-api.py',
                                   f"--config-dir=tests/data/config"])
    time.sleep(2)
    start_server(5000, 'tests/data/server/metadata/', f'{SCRIPT_PATH}/md.log')
    url = f"http://localhost:5701/modelData/"
    full_path = f"tests/data/input/data_submission/{file['name']}"
    res = requests.put(url, data=file, files={'file': open(full_path, 'rb')})
    res.raise_for_status()
    web_server.send_signal(signal.SIGINT)
    kill_pid()
    remove_dir(api_files_dir)


if __name__ == "__main__":
    main()
