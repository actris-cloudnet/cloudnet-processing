#!/usr/bin/env python3
import subprocess
from os import path
import time
import shutil
import requests
from test_utils.utils import start_server, remove_dir
import signal

SCRIPT_PATH = path.dirname(path.realpath(__file__))

test_files = [{
    'name': 'chm15k_20200405.nc',
    'hash': '1ed679842745d1b1809c63d203a0c2bfd2f2b43e15123b306ea9f7ec4c089b71'
},{
    'name': '20200405_granada_ecmwf.nc',
    'hash': '8ea4732a1234f66c374a518f2be041c15087139341103c9c3cbf8680de507424'
},{
    'name': '200405_020000_P06_ZEN.LV1',
    'hash': '539969323e9a8cfb605b6407146f46344f8355691094c3a8d57b4249a51847af'
}]


def main():

    web_server = subprocess.Popen(['python3', 'scripts/run-data-submission-api.py',
                                   f"--config-dir=tests/data/config"])
    time.sleep(3)

    remove_dir('tests/data/input/granada/')

    # files = {'file_submitted': (full_path,
    #                             open(full_path, 'rb'),
    #                             'application/x-netcdf',
    #                             {'Content-Type': 'multipart/form-data', 'accept': 'application/json'})
    #          }

    start_server(5000, 'tests/data/server/metadata/', f'{SCRIPT_PATH}/md.log')
    start_server(5001, 'tests/data/server/pid/', f'{SCRIPT_PATH}/pid.log')

    for file in test_files:
        url = f"http://localhost:5701/data/{file['hash']}"
        full_path = f"tests/data/input/data_submission/{file['name']}"
        res = requests.put(url, data=open(full_path))
        print(res.status_code, res.text)

    web_server.send_signal(signal.SIGINT)


if __name__ == "__main__":
    main()
