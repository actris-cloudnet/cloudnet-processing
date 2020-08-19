#!/usr/bin/env python3
import subprocess
from os import path
import time
import shutil
import requests
from test_utils.utils import start_server, remove_dir
import signal

SCRIPT_PATH = path.dirname(path.realpath(__file__))

test_file = {
    'name': 'chm15k_20200405.nc',
    'hash': '1ed679842745d1b1809c63d203a0c2bfd2f2b43e15123b306ea9f7ec4c089b71'
}


def main():

    web_server = subprocess.Popen(['python3', 'scripts/run-data-submission-api.py',
                                   f"--config-dir=tests/data/config"])
    time.sleep(3)

    shutil.copyfile(f"tests/data/input/{test_file['name']}",
                    f"tests/data/api_input/{test_file['name']}")
    remove_dir('tests/data/input/granada/')

    url = f"http://localhost:5700/data/{test_file['hash']}"
    full_path = f"tests/data/api_input/{test_file['name']}"
    files = {'file_submitted': (full_path,
                                open(full_path, 'rb'),
                                'application/x-netcdf',
                                {'Content-Type': 'multipart/form-data', 'accept': 'application/json'})
             }

    start_server(5000, 'tests/data/server/metadata/', f'{SCRIPT_PATH}/md.log')

    res = requests.put(url, files=files)
    print(res.status_code, res.text)
    remove_dir('tests/data/input/granada/')

    web_server.send_signal(signal.SIGINT)


if __name__ == "__main__":
    main()
