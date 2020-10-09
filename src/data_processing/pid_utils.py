from configparser import SectionProxy
from typing import Tuple

import netCDF4
import requests
from requests import HTTPError


class PidUtils:

    def __init__(self, config: SectionProxy):
        self._pid_service_url = config['url']

    def add_pid_to_file(self, filepath: str) -> Tuple[str, str]:
        """Queries PID service and adds the PID to NC file metadata."""
        rootgrp = netCDF4.Dataset(filepath, 'r+')
        uuid = getattr(rootgrp, 'file_uuid')
        payload = {
            'type': 'file',
            'uuid': uuid
        }
        res = requests.post(self._pid_service_url, json=payload)

        try:
            res.raise_for_status()
        except HTTPError:
            raise HTTPError(f'PID service failed with status {res.status_code}:\n{res.json()["detail"]}')

        pid = res.json()['pid']
        rootgrp.pid = pid
        rootgrp.close()

        return uuid, pid
