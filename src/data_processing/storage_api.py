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
                 config: dict,
                 session: requests.Session = requests.Session()):

        self.url = config['STORAGE-SERVICE']['url']
        self.auth = (config['STORAGE-SERVICE']['username'], config['STORAGE-SERVICE']['password'])
        self.session = session
        self._temp_file = NamedTemporaryFile(suffix='.png')

    def download_products(self, s3keys: list, dir_name: str, volatile: bool = False) -> list:
        """From a list of s3keys, download product files.

        Args:
            s3keys (list): List of s3 keys to be downloaded.
            dir_name (str): Local directory.
            volatile (bool, optional): If True, downloaded products are volatile. Default is False.

        """
        bucket = utils.get_product_bucket(volatile)
        urls = [path.join(self.url, bucket, key) for key in s3keys]
        full_paths = [path.join(dir_name, key) for key in s3keys]
        return self._get_urls(urls, full_paths)

    def download_raw_files(self, metadata: list, dir_name: str) -> list:
        """From a list of upload-metadata, download raw files."""
        urls = [path.join(self.url, 'cloudnet-upload', row['s3key']) for row in metadata]
        full_paths = [path.join(dir_name, row['filename']) for row in metadata]
        return self._get_urls(urls, full_paths)

    def upload_product(self, full_path: str, s3key: str, volatile: bool = False) -> dict:
        """Upload a processed Cloudnet file."""
        bucket = utils.get_product_bucket(volatile)
        headers = self._get_headers(full_path)
        url = path.join(self.url, bucket, s3key)
        res = self.put(url, full_path, headers).json()
        return {'version': res.get('version', ''),
                'size': int(res['size'])}

    def delete_volatile_product(self, s3key: str):
        bucket = utils.get_product_bucket(volatile=True)
        url = path.join(self.url, bucket, s3key)
        res = self.session.delete(url, auth=self.auth)
        return res

    def create_and_upload_images(self, nc_file_full_path: str, product_key: str, uuid: str) -> list:
        product = product_key.split('_')[-1][:-3]
        fields, max_alt = utils.get_fields_for_plot(product)
        visualizations = []
        for field in fields:
            generate_figure(nc_file_full_path, [field], show=False, image_name=self._temp_file.name,
                            max_y=max_alt, sub_title=False, title=False, dpi=120)
            s3key = product_key.replace('.nc', f"-{uuid[:8]}-{field}.png")
            url = path.join(self.url, 'cloudnet-img', s3key)
            headers = self._get_headers(self._temp_file.name)
            self.put(url, self._temp_file.name, headers=headers)
            visualizations.append({
                's3key': s3key,
                'variable_id': utils.get_var_id(product, field),
            })
        return visualizations

    def put(self, url: str, full_path: str, headers: Union[dict, None] = None) -> requests.Response:
        """Upload file to S3 archive."""
        res = self.session.put(url, data=open(full_path, 'rb'), auth=self.auth, headers=headers)
        res.raise_for_status()
        return res

    def get(self, url: str, full_path: str) -> requests.Response:
        """Download file from S3 archive.

        Args:
            url (str): URL at S3 of the file to be downloaded.
            full_path (str): Full path for the file to be saved on the local disk.

        """
        res = self.session.get(url, auth=self.auth)
        res.raise_for_status()
        with open(full_path, 'wb') as f:
            f.write(res.content)
        return res

    @staticmethod
    def _get_headers(full_path):
        checksum = utils.md5sum(full_path, is_base64=True)
        return {'content-md5': checksum}

    def _get_urls(self, urls: list, full_paths: list) -> list:
        for args in zip(urls, full_paths):
            self.get(*args)
        return full_paths

