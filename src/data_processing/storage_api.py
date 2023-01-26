"""Metadata API for Cloudnet files."""
import hashlib
from os import path
from typing import Literal

import requests

from data_processing import utils


class StorageApiException(Exception):
    pass


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

    def download_raw_data(self, metadata: list, dir_name: str) -> tuple[list, list, list]:
        """Download raw files."""
        urls = [path.join(self._url, row["s3path"][1:]) for row in metadata]
        full_paths = [path.join(dir_name, row["filename"]) for row in metadata]
        for row, url, full_path in zip(metadata, urls, full_paths):
            self._get(url, full_path, int(row["size"]), row["checksum"], "md5")
        uuids = [row["uuid"] for row in metadata]
        instrument_pids = [row["instrumentPid"] for row in metadata if "instrumentPid" in row]
        return full_paths, uuids, instrument_pids

    def download_product(self, metadata: dict, dir_name: str) -> str:
        """Download a product."""
        filename = metadata["filename"]
        s3key = f"legacy/{filename}" if metadata.get("legacy", False) is True else filename
        bucket = utils.get_product_bucket(metadata["volatile"])
        url = path.join(self._url, bucket, s3key)
        full_path = path.join(dir_name, filename)
        self._get(url, full_path, int(metadata["size"]), metadata["checksum"], "sha256")
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

    def _put(self, url: str, full_path: str, headers: dict | None = None) -> requests.Response:
        res = self.session.put(url, data=open(full_path, "rb"), auth=self._auth, headers=headers)
        res.raise_for_status()
        return res

    def _get(
        self,
        url: str,
        full_path: str,
        size: int,
        checksum: str,
        checksum_algorithm: Literal["md5", "sha256"],
    ):
        res_size = 0
        hash_sum = getattr(hashlib, checksum_algorithm)()
        with open(full_path, "wb") as output:
            with self.session.get(url, auth=self._auth, stream=True) as res:
                res.raise_for_status()
                for chunk in res.iter_content(chunk_size=8192):
                    output.write(chunk)
                    hash_sum.update(chunk)
                    res_size += len(chunk)
        if res_size != size:
            raise StorageApiException(f"Invalid size: expected {size} bytes, got {res_size} bytes)")
        if (res_checksum := hash_sum.hexdigest()) != checksum:
            raise StorageApiException(
                f"Invalid checksum: expected {checksum} bytes, got {res_checksum} bytes)"
            )

    @staticmethod
    def _get_headers(full_path: str) -> dict:
        checksum = utils.md5sum(full_path, is_base64=True)
        return {"content-md5": checksum}
