from os import PathLike

import netCDF4
import requests
from requests import HTTPError

from data_processing.config import Config
from data_processing.utils import MiscError, random_string

from .utils import build_file_landing_page_url, make_session


class PidUtils:
    def __init__(self, config: Config, session: requests.Session | None = None):
        self._pid_service_url = config.pid_service_url
        self._is_production = config.is_production
        if session is None:
            session = make_session()
        self.session = session

    def add_pid_to_file(self, filepath: PathLike | str) -> tuple[str, str, str]:
        """Queries PID service and adds the PID to NC file metadata."""
        with netCDF4.Dataset(filepath, "r+") as rootgrp:
            uuid = getattr(rootgrp, "file_uuid")
            url = build_file_landing_page_url(uuid)
            if self._is_production:
                payload = {
                    "type": "file",
                    "uuid": uuid,
                    "url": url,
                }
                res = self.session.post(self._pid_service_url, json=payload)
                try:
                    res.raise_for_status()
                except HTTPError as exc:
                    raise MiscError(
                        f'PID service failed with status {res.status_code}:\n{res.json()["detail"]}'
                    ) from exc
                pid = res.json()["pid"]
            else:
                pid = f"https://www.example.pid/{random_string(5)}"
            rootgrp.pid = pid

        return uuid, pid, url
