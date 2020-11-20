"""Metadata API for Cloudnet files."""
from os import path
import requests
from data_processing import utils
from cloudnetpy.plotting import generate_figure
from tempfile import NamedTemporaryFile
from typing import Union


class StorageApi:
    """Class for uploading / downloading files from the Cloudnet data archive in Sodankylä."""

    def __init__(self,
                 url: str,
                 auth: tuple,
                 product_bucket: str,
                 session: requests.Session = requests.Session()):
        self.url = url
        self.auth = auth
        self.product_bucket = product_bucket
        self.session = session
        self._temp_file = NamedTemporaryFile(suffix='.png')

    def download_raw_files(self, metadata: list, dir_name: str) -> list:
        """From a list of upload-metadata, download files."""
        urls = [path.join(self.url, 'cloudnet-upload', row['s3Key']) for row in metadata]
        full_paths = [path.join(dir_name, row['filename']) for row in metadata]
        for args in zip(urls, full_paths):
            self.get(*args)
        return full_paths

    def upload_product(self, full_path: str, key: str) -> dict:
        """Upload a processed Cloudnet file."""
        headers = self._get_headers(full_path)
        url = path.join(self.url, self.product_bucket, key)
        res = self.put(url, full_path, headers).json()
        return {'version': res.get('version', 'volatile'),
                'size': res['size']}

    def create_images(self, nc_file_full_path: str, product_key: str, file_info: dict) -> None:
        product = product_key.split('_')[-1][:-3]
        fields, max_alt = utils.get_fields_for_plot(product)
        for field in fields:
            generate_figure(nc_file_full_path, [field], show=False, image_name=self._temp_file.name,
                            max_y=max_alt, sub_title=False, title=False, dpi=120)
            key = product_key.replace('.nc', f"-{file_info['version']}-{field}.png")
            url = path.join(self.url, 'cloudnet-img', key)
            headers = self._get_headers(self._temp_file.name)
            self.put(url, self._temp_file.name, headers=headers)

    @staticmethod
    def _get_headers(full_path):
        checksum = utils.md5sum(full_path, is_base64=True)
        return {'content-md5': checksum}

    def put(self, url: str, full_path: str, headers: Union[dict, None] = None) -> requests.Response:
        """Upload file to S3 archive."""
        res = self.session.put(url, data=open(full_path, 'rb'), auth=self.auth, headers=headers)
        res.raise_for_status()
        return res

    def get(self, url: str, full_path: str) -> requests.Response:
        """Download file from S3 archive."""
        res = self.session.get(url, auth=self.auth)
        res.raise_for_status()
        with open(full_path, 'wb') as f:
            f.write(res.content)
        return res
