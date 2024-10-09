"""Metadata API for Cloudnet files."""

import hashlib
import logging
import uuid
from os import PathLike
from pathlib import Path
from typing import Literal

import requests

from processing import utils
from processing.config import Config


class StorageApi:
    """Class for uploading and downloading files from the Cloudnet S3 data archive."""

    def __init__(self, config: Config, session: requests.Session):
        self.session = session
        self._url = config.storage_service_url
        self._auth = config.storage_service_auth

    def upload_product(
        self, full_path: PathLike | str, s3key: str, volatile: bool
    ) -> dict:
        """Upload a processed Cloudnet file."""
        bucket = _get_product_bucket(volatile)
        headers = self._get_headers(full_path)
        url = f"{self._url}/{bucket}/{s3key}"
        res = self._put(url, full_path, headers).json()
        return {"version": res.get("version", ""), "size": int(res["size"])}

    def download_raw_data(
        self, metadata: list, dir_name: PathLike | str
    ) -> tuple[list[Path], list[uuid.UUID]]:
        """Download raw instrument or model files."""
        urls = [f"{self._url}/cloudnet-upload/{row['s3key']}" for row in metadata]
        full_paths = [Path(dir_name) / row["filename"] for row in metadata]
        for row, url, full_path in zip(metadata, urls, full_paths):
            self._get(url, full_path, int(row["size"]), row["checksum"], "md5")
        uuids = [uuid.UUID(row["uuid"]) for row in metadata]
        instrument_pids = [
            row["instrumentPid"] for row in metadata if "instrumentPid" in row
        ]
        if instrument_pids:
            assert len(list(set(instrument_pids))) == 1
        return full_paths, uuids

    def download_product(self, metadata: dict, dir_name: PathLike | str) -> Path:
        """Download a product."""
        filename = metadata["filename"]
        s3key = (
            f"legacy/{filename}" if metadata.get("legacy", False) is True else filename
        )
        bucket = _get_product_bucket(metadata["volatile"])
        url = f"{self._url}/{bucket}/{s3key}"
        full_path = Path(dir_name) / filename
        self._get(url, full_path, int(metadata["size"]), metadata["checksum"], "sha256")
        return full_path

    def delete_volatile_product(self, s3key: str) -> requests.Response:
        """Delete a volatile product."""
        bucket = _get_product_bucket(volatile=True)
        url = f"{self._url}/{bucket}/{s3key}"
        res = self.session.delete(url, auth=self._auth)
        return res

    def upload_image(self, full_path: str | PathLike, s3key: str) -> None:
        url = f"{self._url}/cloudnet-img/{s3key}"
        headers = self._get_headers(full_path)
        self._put(url, full_path, headers=headers)

    def _put(
        self, url: str, full_path: str | PathLike, headers: dict | None = None
    ) -> requests.Response:
        res = self.session.put(
            url, data=open(full_path, "rb"), auth=self._auth, headers=headers
        )
        res.raise_for_status()
        return res

    def _get(
        self,
        url: str,
        full_path: str | PathLike,
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
            logging.warning(
                "Invalid size: expected %d bytes, got %d bytes", size, res_size
            )
        if (res_checksum := hash_sum.hexdigest()) != checksum:
            logging.warning(
                "Invalid checksum: expected %s, got %s", checksum, res_checksum
            )

    @staticmethod
    def _get_headers(full_path: str | PathLike) -> dict:
        checksum = utils.md5sum(full_path, is_base64=True)
        return {"content-md5": checksum}


def _get_product_bucket(volatile: bool = False) -> str:
    return "cloudnet-product-volatile" if volatile else "cloudnet-product"
