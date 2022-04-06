"""Metadata API for Cloudnet files."""
from os import path
from typing import Optional, Tuple

import requests

from cloudnet_processing import utils


class StorageApi:
    """Class for uploading / downloading files from the Cloudnet S3 data archive in Sodankylä."""

    def __init__(self, config: dict, session: requests.Session):
        self.session = session
        self._url = config["STORAGE_SERVICE_URL"]
        self._auth = (config["STORAGE_SERVICE_USER"], config["STORAGE_SERVICE_PASSWORD"])

    def upload_product(self, full_path: str, s3key: str) -> dict:
        """Upload a processed Cloudnet file."""
        volatile = utils.is_volatile_file(full_path)
        bucket = utils.get_product_bucket(volatile)
        headers = self._get_headers(full_path)
        url = path.join(self._url, bucket, s3key)
        res = self._put(url, full_path, headers).json()
        return {"version": res.get("version", ""), "size": int(res["size"])}

    def download_raw_files(self, metadata: list, dir_name: str) -> Tuple[list, list]:
        """Download raw files."""
        urls = [path.join(self._url, row["s3path"][1:]) for row in metadata]
        full_paths = [path.join(dir_name, row["filename"]) for row in metadata]
        for args in zip(urls, full_paths):
            self._get(*args)
        uuids = [row["uuid"] for row in metadata]
        return full_paths, uuids

    def download_product(self, metadata: dict, dir_name: str) -> str:
        """Download a product."""
        filename = metadata["filename"]
        s3key = f"legacy/{filename}" if metadata.get("legacy", False) is True else filename
        bucket = utils.get_product_bucket(metadata["volatile"])
        url = path.join(self._url, bucket, s3key)
        full_path = path.join(dir_name, filename)
        self._get(url, full_path)
        return full_path

    def delete_volatile_product(self, s3key: str) -> requests.Response:
        """Delete a volatile product."""
        bucket = utils.get_product_bucket(volatile=True)
        url = path.join(self._url, bucket, s3key)
        res = self.session.delete(url, auth=self._auth)
        return res

    def upload_image(self, full_path: str, s3key: str) -> None:
        url = path.join(self._url, "cloudnet-img", s3key)
        headers = self._get_headers(full_path)
        self._put(url, full_path, headers=headers)

    def _put(self, url: str, full_path: str, headers: Optional[dict] = None) -> requests.Response:
        res = self.session.put(url, data=open(full_path, "rb"), auth=self._auth, headers=headers)
        res.raise_for_status()
        return res

    def _get(self, url: str, full_path: str) -> requests.Response:
        res = self.session.get(url, auth=self._auth)
        res.raise_for_status()
        with open(full_path, "wb") as f:
            f.write(res.content)
        return res

    @staticmethod
    def _get_headers(full_path: str) -> dict:
        checksum = utils.md5sum(full_path, is_base64=True)
        return {"content-md5": checksum}
