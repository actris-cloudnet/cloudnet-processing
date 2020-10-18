#!/usr/bin/env python3
import subprocess
import os
from os import path
import time
import requests
from test_utils.utils import start_server, remove_dir, kill_pid
import signal
import pytest
from data_processing.utils import sha256sum

SCRIPT_PATH = path.dirname(path.realpath(__file__))

data_dir = "tests/data/input/data_submission/"


def main():

    api_files_dir = 'tests/data/api_files/'
    remove_dir(api_files_dir)

    web_server = subprocess.Popen(['python3', 'scripts/run-data-submission-api.py',
                                   f"--config-dir=tests/data/config"])
    time.sleep(3)

    start_server(5000, 'tests/data/server/metadata/modelFiles/', f'{SCRIPT_PATH}/md.log')

    url = f"http://localhost:5701/modelData/"

    filenames = ('20200405_granada_ecmwf.nc', 'chm15k_20200405.nc', '200405_020000_P06_ZEN.LV1')

    for filename in filenames:
        filepath = f"{data_dir}{filename}"
        meta = {'hashSum': sha256sum(filepath), 'modelType': 'ecmwf'}
        requests.post(url, data=meta, files={'file': open(filepath, 'rb')})

    pytest.main(['-v', 'tests/e2e/model_data_submission/tests.py'])

    web_server.send_signal(signal.SIGINT)
    kill_pid()
    remove_dir(api_files_dir)

    os.remove(f'tests/data/freeze/model/20200405_granada_ecmwf.nc')


if __name__ == "__main__":
    main()
