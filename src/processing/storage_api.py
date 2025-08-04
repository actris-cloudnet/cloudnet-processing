"""Metadata API for Cloudnet files."""

import concurrent
import concurrent.futures
import hashlib
import logging
import re
import threading
import uuid
from os import PathLike
from pathlib import Path
from typing import Iterable

import requests

from processing import utils
from processing.config import Config


class StorageApiError(Exception):
    pass


class StorageApi:
    """Class for uploading and downloading files from the Cloudnet S3 data archive."""

    def __init__(self, config: Config, session: requests.Session):
        self.session = session
        self.config = config
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
        full_paths = self._download_parallel(
            metadata, checksum_algorithm="md5", output_directory=Path(dir_name)
        )
        uuids = [uuid.UUID(row["uuid"]) for row in metadata]
        instrument_pids = [
            row["instrument"]["pid"] for row in metadata if "instrument" in row
        ]
        if instrument_pids:
            assert len(list(set(instrument_pids))) == 1
        return full_paths, uuids

    def download_product(self, metadata: dict, dir_name: PathLike | str) -> Path:
        """Download a product."""
        filename = metadata["filename"]
        full_path = Path(dir_name) / filename
        _download_url(
            url=self._get_download_url(metadata),
            size=int(metadata["size"]),
            checksum=metadata["checksum"],
            checksum_algorithm="sha256",
            output_path=full_path,
            auth=self._auth,
        )
        return full_path

    def download_products(
        self, meta_records: Iterable[dict], dir_name: PathLike | str
    ) -> list[Path]:
        """Download multiple products."""
        return self._download_parallel(
            meta_records, checksum_algorithm="sha256", output_directory=Path(dir_name)
        )

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

    @staticmethod
    def _get_headers(full_path: str | PathLike) -> dict:
        checksum = utils.md5sum(full_path, is_base64=True)
        return {"content-md5": checksum}

    def _download_parallel(
        self,
        meta_records: Iterable[dict],
        checksum_algorithm: str,
        output_directory: Path,
    ) -> list[Path]:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        try:
            futures = []
            paths = []
            unique_urls = set()
            for meta in meta_records:
                filename = meta["filename"]
                path = output_directory / filename
                paths.append(path)
                url = self._get_download_url(meta)
                if url in unique_urls:
                    continue
                unique_urls.add(url)
                future = executor.submit(
                    _download_url,
                    url=url,
                    size=int(meta["size"]),
                    checksum=meta["checksum"],
                    checksum_algorithm=checksum_algorithm,
                    output_path=path,
                    auth=self._auth,
                )
                futures.append(future)
            done, not_done = concurrent.futures.wait(
                futures, timeout=60 * 60, return_when=concurrent.futures.FIRST_EXCEPTION
            )
            for future in done:
                if exc := future.exception():
                    raise StorageApiError("Failed to download all files") from exc
            return paths
        finally:
            executor.shutdown(cancel_futures=True)

    def _get_download_url(self, metadata: dict) -> str:
        return re.sub(
            r".*/api/download/",
            self.config.dataportal_url + "/api/download/",
            metadata["downloadUrl"],
        )


def _get_product_bucket(volatile: bool = False) -> str:
    return "cloudnet-product-volatile" if volatile else "cloudnet-product"


thread_local = threading.local()


def _download_url(
    url: str,
    size: int,
    checksum: str,
    checksum_algorithm: str,
    output_path: Path,
    auth: tuple[str, str],
):
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    res_size = 0
    hash_sum = hashlib.new(checksum_algorithm)
    with output_path.open("wb") as output_file:
        with thread_local.session.get(
            url, auth=auth, timeout=2 * 60, stream=True
        ) as res:
            res.raise_for_status()
            for chunk in res.iter_content(chunk_size=8192):
                output_file.write(chunk)
                hash_sum.update(chunk)
                res_size += len(chunk)
    if res_size != size:
        logging.warning("Invalid size: expected %d bytes, got %d bytes", size, res_size)
    if (res_checksum := hash_sum.hexdigest()) != checksum:
        logging.warning("Invalid checksum: expected %s, got %s", checksum, res_checksum)
