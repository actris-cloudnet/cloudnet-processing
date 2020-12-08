"""Metadata API for Cloudnet files."""
from os import path
import requests
from data_processing import utils
from cloudnetpy.plotting import generate_figure
from tempfile import NamedTemporaryFile
from typing import Union


class StorageApi:
    """Class for uploading / downloading files from the Cloudnet S3 data archive in Sodankylä."""

    def __init__(self, config: dict, session=requests.Session()):
        self.session = session
        self._url = config['STORAGE-SERVICE']['url']
        self._auth = (config['STORAGE-SERVICE']['username'],
                      config['STORAGE-SERVICE']['password'])

    def upload_product(self, full_path: str, s3key: str) -> dict:
        """Upload a processed Cloudnet file."""
        volatile = utils.is_volatile_file(full_path)
        bucket = utils.get_product_bucket(volatile)
        headers = self._get_headers(full_path)
        url = path.join(self._url, bucket, s3key)
        res = self._put(url, full_path, headers).json()
        return {'version': res.get('version', ''),
                'size': int(res['size'])}

    def download_raw_files(self, metadata: list, dir_name: str) -> list:
        """Download raw files."""
        urls = [path.join(self._url, 'cloudnet-upload', row['s3key']) for row in metadata]
        full_paths = [path.join(dir_name, row['filename']) for row in metadata]
        for args in zip(urls, full_paths):
            self._get(*args)
        return full_paths

    def download_product(self, metadata: dict, dir_name: str) -> str:
        """Download a product."""
        s3key = metadata['filename']
        bucket = utils.get_product_bucket(metadata['volatile'])
        url = path.join(self._url, bucket, s3key)
        full_path = path.join(dir_name, s3key)
        self._get(url, full_path)
        return full_path

    def delete_volatile_product(self, s3key: str) -> requests.Response:
        """Delete a volatile product."""
        bucket = utils.get_product_bucket(volatile=True)
        url = path.join(self._url, bucket, s3key)
        res = self.session.delete(url, auth=self._auth)
        return res

    def create_and_upload_images(self,
                                 nc_file_full_path: str,
                                 product_key: str,
                                 uuid: str,
                                 product: str) -> list:
        """Create and upload images."""
        temp_file = NamedTemporaryFile(suffix='.png')
        try:
            fields, max_alt = utils.get_fields_for_plot(product)
        except NotImplementedError:
            print(f'Warning: plotting for {product} not implemented', end='\t')
            return []
        visualizations = []
        for field in fields:
            generate_figure(nc_file_full_path, [field], show=False, image_name=temp_file.name,
                            max_y=max_alt, sub_title=False, title=False, dpi=120)
            s3key = product_key.replace('.nc', f"-{uuid[:8]}-{field}.png")
            url = path.join(self._url, 'cloudnet-img', s3key)
            headers = self._get_headers(temp_file.name)
            self._put(url, temp_file.name, headers=headers)
            visualizations.append({
                's3key': s3key,
                'variable_id': utils.get_var_id(product, field),
            })
        return visualizations

    def _put(self, url: str, full_path: str,
             headers: Union[dict, None] = None) -> requests.Response:
        res = self.session.put(url, data=open(full_path, 'rb'), auth=self._auth, headers=headers)
        res.raise_for_status()
        return res

    def _get(self, url: str, full_path: str) -> requests.Response:
        res = self.session.get(url, auth=self._auth)
        res.raise_for_status()
        with open(full_path, 'wb') as f:
            f.write(res.content)
        return res

    @staticmethod
    def _get_headers(full_path: str) -> dict:
        checksum = utils.md5sum(full_path, is_base64=True)
        return {'content-md5': checksum}
