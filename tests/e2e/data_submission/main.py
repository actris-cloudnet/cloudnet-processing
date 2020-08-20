#!/usr/bin/env python3
import subprocess
from os import path
import time
import shutil

import pytest
import requests
from test_utils.utils import start_server, remove_dir
import signal

SCRIPT_PATH = path.dirname(path.realpath(__file__))

test_files = [{
    'name': 'chm15k_20200405.nc',
    'hashSum': '1ed679842745d1b1809c63d203a0c2bfd2f2b43e15123b306ea9f7ec4c089b71',
    "measurementDate":"2020-04-05",
    "product":"radar",
    "site": "granada"
},{
    'name': '20200405_granada_ecmwf.nc',
    'hashSum': '8ea4732a1234f66c374a518f2be041c15087139341103c9c3cbf8680de507424',
    "measurementDate": "2020-04-05",
    "product": "model",
    "site": "granada"
},{
    'name': '200405_020000_P06_ZEN.LV1',
    'hashSum': '539969323e9a8cfb605b6407146f46344f8355691094c3a8d57b4249a51847af',
    "measurementDate": "2020-04-05",
    "product": "radar",
    "site": "granada"
}]


def main():

    web_server = subprocess.Popen(['python3', 'scripts/run-data-submission-api.py',
                                   f"--config-dir=tests/data/config"])
    time.sleep(3)

    remove_dir('tests/data/input/granada/')

    start_server(5000, 'tests/data/server/metadata/', f'{SCRIPT_PATH}/md.log')
    start_server(5001, 'tests/data/server/pid/', f'{SCRIPT_PATH}/pid.log')

    for file in test_files:
        url = f"http://localhost:5701/data/"
        full_path = f"tests/data/input/data_submission/{file['name']}"
        del file['name']
        res = requests.post(url, data=file, files={'file_submitted': open(full_path, 'rb')}, auth=('granada', 'letmein'))
        print(res.status_code, res.text)

    web_server.send_signal(signal.SIGINT)

    pytest.main(['-v', 'tests/e2e/data_submission/tests.py'])

    remove_dir('tests/data/input/granada/')


if __name__ == "__main__":
    main()
