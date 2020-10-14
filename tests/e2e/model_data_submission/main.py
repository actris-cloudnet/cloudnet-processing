#!/usr/bin/env python3
import subprocess
from os import path
import time
import requests
from test_utils.utils import start_server, remove_dir, kill_pid
import signal
import pytest

SCRIPT_PATH = path.dirname(path.realpath(__file__))

filepath = "tests/data/input/data_submission/20200405_granada_ecmwf.nc"

meta = {'hashSum': '8ea4732a1234f66c374a518f2be041c15087139341103c9c3cbf8680de507424',
        'modelType': 'ecmwf'}


def main():

    api_files_dir = 'tests/data/api_files/'
    remove_dir(api_files_dir)

    web_server = subprocess.Popen(['python3', 'scripts/run-data-submission-api.py',
                                   f"--config-dir=tests/data/config"])
    time.sleep(3)

    start_server(5000, 'tests/data/server/metadata/modelFiles/', f'{SCRIPT_PATH}/md.log')

    url = f"http://localhost:5701/modelData/"
    res = requests.put(url, data=meta, files={'file': open(filepath, 'rb')})
    res.raise_for_status()

    pytest.main(['-v', 'tests/e2e/model_data_submission/tests.py'])

    web_server.send_signal(signal.SIGINT)
    kill_pid()
    remove_dir(api_files_dir)


if __name__ == "__main__":
    main()
