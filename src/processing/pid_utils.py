import logging
import random
import string
from pathlib import Path

import netCDF4
import requests
from requests import HTTPError

from processing.config import Config
from processing.utils import MiscError

from .utils import build_file_landing_page_url, make_session


class PidUtils:
    def __init__(self, config: Config, session: requests.Session | None = None) -> None:
        self._pid_service_url = f"{config.pid_service_url}/pid/"
        self._is_production = config.is_production
        if session is None:
            session = make_session()
        self.session = session

    def add_pid_to_file(
        self, filepath: Path, pid: str | None = None
    ) -> tuple[str, str | None, str]:
        """Queries PID service and adds the PID to NC file metadata."""
        with netCDF4.Dataset(filepath, "r+") as rootgrp:
            uuid = getattr(rootgrp, "file_uuid")
            url = build_file_landing_page_url(uuid)
            pid_to_file: str | None
            if pid:
                pid_to_file = pid
            elif self._is_production:
                pid_to_file = self._request_pid(uuid, url)
            else:
                pid_to_file = f"https://www.example.pid/{_random_string(5)}"
            if pid_to_file is not None:
                rootgrp.pid = pid_to_file

        return uuid, pid_to_file, url

    def _request_pid(self, uuid: str, url: str) -> str | None:
        payload = {
            "type": "file",
            "uuid": uuid,
            "url": url,
        }
        try:
            res = self.session.post(self._pid_service_url, json=payload)
        except (requests.ConnectionError, requests.ReadTimeout):
            logging.warning("PID service unavailable, generating file without PID")
            return None
        try:
            res.raise_for_status()
        except HTTPError as exc:
            try:
                error = res.json()["detail"]
            except (requests.JSONDecodeError, KeyError):
                error = res.text[:100]
            raise MiscError(
                f"PID service failed with status {res.status_code}:\n{error}"
            ) from exc
        return res.json()["pid"]


def _random_string(n: int = 10) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=n))
