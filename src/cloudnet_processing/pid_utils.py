from typing import Tuple

import netCDF4
import requests
from requests import HTTPError


class PidUtils:
    def __init__(self, config: dict):
        self._pid_service_url = config["PID_SERVICE_URL"]
        self._is_production = self._get_env(config)

    @staticmethod
    def _get_env(config) -> bool:
        is_test_env = config["PID_SERVICE_TEST_ENV"].lower() == "true"
        return not is_test_env

    def add_pid_to_file(self, filepath: str) -> Tuple[str, str]:
        """Queries PID service and adds the PID to NC file metadata."""
        rootgrp = netCDF4.Dataset(filepath, "r+")
        uuid = getattr(rootgrp, "file_uuid")
        if self._is_production:
            payload = {"type": "file", "uuid": uuid}
            res = requests.post(self._pid_service_url, json=payload)
            try:
                res.raise_for_status()
            except HTTPError:
                raise HTTPError(
                    f'PID service failed with status {res.status_code}:\n{res.json()["detail"]}'
                )
            pid = res.json()["pid"]
        else:
            pid = "https://www.example.pid/"
        rootgrp.pid = pid
        rootgrp.close()

        return uuid, pid
