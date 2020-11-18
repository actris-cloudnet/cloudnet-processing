"""Metadata API for Cloudnet files."""
from typing import Tuple
from os import path
import requests


class StorageApi:
    """Class handling connection between Cloudnet files and database."""

    def __init__(self, url, session=requests.Session()):
        self.url = url
        self.session = session

    def download_files(self, metadata: list, dir_name: str) -> Tuple[list, list]:
        """From a list of upload-metadata, download files."""
        if len(metadata) == 0:
            raise ValueError
        print('Downloading files from S3...')
        full_paths = []
        checksums = []
        for row in metadata:
            download_url = path.join(self.url, 'cloudnet-upload', row['s3Key'])
            res = requests.get(download_url)
            if res.status_code == 200:
                full_path = path.join(dir_name, row['filename'])
                with open(full_path, 'wb') as f:
                    f.write(res.content)
                full_paths.append(full_path)
                checksums.append(row['checksum'])
        return full_paths, checksums
