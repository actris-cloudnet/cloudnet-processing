"""Metadata API for Cloudnet files."""

import logging
import uuid

import requests

from processing import utils
from processing.config import Config


class MetadataApi:
    """Class handling connection between Cloudnet files and database."""

    def __init__(
        self, config: Config, session: requests.Session = utils.make_session()
    ):
        self.config = config
        self.session = session
        self._url = config.dataportal_url
        self._auth = config.data_submission_auth

    def get(self, end_point: str, payload: dict | None = None, json: bool = True):
        """Get Cloudnet metadata."""
        url = f"{self._url}/{end_point}"
        res = self.session.get(url, params=payload)
        res.raise_for_status()
        if json:
            return res.json()
        return res

    def post(
        self, end_point: str, payload: dict, auth: tuple[str, str] | None = None
    ) -> requests.Response:
        """Update upload / product metadata."""
        url = f"{self._url}/{end_point}"
        res = self.session.post(url, json=payload, auth=auth)
        res.raise_for_status()
        return res

    def put(self, end_point: str, resource: str, payload: dict) -> requests.Response:
        """PUT metadata to Cloudnet data portal."""
        url = f"{self._url}/{end_point}/{resource}"
        res = self.session.put(url, json=payload)
        res.raise_for_status()
        return res

    def _put_file(
        self, end_point: str, resource: str, full_path: str, auth: tuple[str, str]
    ) -> requests.Response:
        """PUT file to Cloudnet data portal."""
        url = f"{self._url}/{end_point}/{resource}"
        res = self.session.put(url, data=open(full_path, "rb"), auth=auth)
        res.raise_for_status()
        return res

    def delete(self, end_point: str, params: dict | None = None) -> requests.Response:
        """Delete Cloudnet metadata."""
        url = f"{self._url}/{end_point}"
        res = self.session.delete(url, auth=self._auth, params=params)
        res.raise_for_status()
        return res

    def put_images(self, img_metadata: list, product_uuid: str | uuid.UUID):
        for data in img_metadata:
            payload = {
                "sourceFileId": str(product_uuid),
                "variableId": data["variable_id"],
                "dimensions": data["dimensions"],
            }
            self.put("visualizations", data["s3key"], payload)

    def upload_instrument_file(
        self, base, instrument: str, filename: str, instrument_pid: str | None = None
    ):
        checksum = utils.md5sum(base.daily_file.name)
        metadata = {
            "filename": filename,
            "checksum": checksum,
            "instrument": instrument,
            "measurementDate": base.date_str,
            "site": base.site,
            "instrumentPid": instrument_pid,
        }
        try:
            self.post("upload/metadata", metadata, auth=self._auth)
        except requests.exceptions.HTTPError as err:
            logging.info(err)
            return
        self._put_file("upload/data", checksum, base.daily_file.name, self._auth)

    def update_dvas_info(self, uuid: uuid.UUID, timestamp: str, dvas_id: str):
        payload = {"uuid": uuid, "dvasUpdatedAt": timestamp, "dvasId": dvas_id}
        self.post("files", payload)

    def clean_dvas_info(self, uuid: uuid.UUID):
        payload = {"uuid": uuid, "dvasUpdatedAt": None, "dvasId": None}
        self.post("files", payload)
