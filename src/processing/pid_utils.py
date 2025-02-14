import random
import string
from os import PathLike

import netCDF4
import requests
from requests import HTTPError

from processing.config import Config
from processing.utils import MiscError

from .utils import build_file_landing_page_url, make_session


class PidUtils:
    def __init__(self, config: Config, session: requests.Session | None = None):
        self._pid_service_url = f"{config.pid_service_url}/pid/"
        self._is_production = config.is_production
        if session is None:
            session = make_session()
        self.session = session

    def add_pid_to_file(
        self, filepath: PathLike | str, pid: str | None = None
    ) -> tuple[str, str, str]:
        """Queries PID service and adds the PID to NC file metadata."""
        with netCDF4.Dataset(filepath, "r+") as rootgrp:
            uuid = getattr(rootgrp, "file_uuid")
            url = build_file_landing_page_url(uuid)
            if pid is not None:
                pid_to_file = pid
            elif self._is_production:
                payload = {
                    "type": "file",
                    "uuid": uuid,
                    "url": url,
                }
                res = self.session.post(self._pid_service_url, json=payload)
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
                pid_to_file = res.json()["pid"]
            else:
                pid_to_file = f"https://www.example.pid/{_random_string(5)}"
            rootgrp.pid = pid_to_file

        return uuid, pid_to_file, url


def _random_string(n: int = 10) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=n))
