"""Metadata API for Cloudnet files."""

import concurrent
import concurrent.futures
import hashlib
import logging
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from uuid import UUID

import requests
from cloudnet_api_client.containers import (
    Metadata,
    ProductMetadata,
    RawMetadata,
    RawModelMetadata,
)
from cloudnet_api_client.utils import md5sum, sha256sum

from processing.config import Config


class StorageApiError(Exception):
    pass


@dataclass
class StorageApiFileInfo:
    version: str
    size: int


class StorageApi:
    """Class for uploading and downloading files from the Cloudnet S3 data archive."""

    def __init__(self, config: Config, session: requests.Session) -> None:
        self.session = session
        self.config = config
        self._url = config.storage_service_url
        self._auth = config.storage_service_auth

    def upload_product(
        self, full_path: Path, uuid: UUID, s3key: str
    ) -> StorageApiFileInfo:
        """Upload a processed Cloudnet file."""
        bucket = "cloudnet-product-new"
        headers = self._get_headers(full_path)
        url = f"{self._url}/{bucket}/{uuid}/{s3key}"
        res = self._put(url, full_path, headers).json()
        return StorageApiFileInfo(version=res.get("version", ""), size=int(res["size"]))

    def upload_image(self, full_path: Path, s3key: str) -> None:
        url = f"{self._url}/cloudnet-img/{s3key}"
        headers = self._get_headers(full_path)
        self._put(url, full_path, headers=headers)

    def _put(
        self, url: str, full_path: Path, headers: dict | None = None
    ) -> requests.Response:
        with full_path.open("rb") as f:
            res = self.session.put(url, data=f, auth=self._auth, headers=headers)
            res.raise_for_status()
            return res

    @staticmethod
    def _get_headers(full_path: Path) -> dict:
        checksum = md5sum(full_path, is_base64=True)
        return {"content-md5": checksum}
