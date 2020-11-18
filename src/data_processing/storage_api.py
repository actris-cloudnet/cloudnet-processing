"""Metadata API for Cloudnet files."""
from typing import Tuple
from os import path
import requests


class StorageApi:
    """Class handling connection between Cloudnet files and database."""

    def __init__(self, url, session=requests.Session()):
        self.url = url
        self.session = session

    def download_files(self, metadata: list, dir_name: str, uploaded_only: bool = True) -> Tuple[list, list]:
        """From a list of upload-metadata, download files."""
        full_paths = []
        checksums = []
        for row in metadata:
            if (uploaded_only and row['status'] == 'uploaded') or not uploaded_only:
                url = path.join(self.url, 'cloudnet-upload', row['s3Key'])
                res = requests.get(url)
                if res.status_code == 200:
                    full_path = path.join(dir_name, row['filename'])
                    with open(full_path, 'wb') as f:
                        f.write(res.content)
                    full_paths.append(full_path)
                    checksums.append(row['checksum'])
        if len(full_paths) == 0:
            raise ValueError
        return full_paths, checksums
