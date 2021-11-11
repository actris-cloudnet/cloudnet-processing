"""Metadata API for Cloudnet files."""
from os import path
from typing import Optional, Tuple
from tempfile import NamedTemporaryFile
import requests
import logging
from cloudnetpy.plotting import generate_figure, generate_legacy_figure
from model_evaluation.plotting.plotting import generate_L3_day_plots
from data_processing import utils


class StorageApi:
    """Class for uploading / downloading files from the Cloudnet S3 data archive in Sodankylä."""

    def __init__(self, config: dict, session: requests.Session):
        self.session = session
        self._url = config['STORAGE_SERVICE_URL']
        self._auth = (config['STORAGE_SERVICE_USER'],
                      config['STORAGE_SERVICE_PASSWORD'])

    def upload_product(self, full_path: str, s3key: str) -> dict:
        """Upload a processed Cloudnet file."""
        volatile = utils.is_volatile_file(full_path)
        bucket = utils.get_product_bucket(volatile)
        headers = self._get_headers(full_path)
        url = path.join(self._url, bucket, s3key)
        res = self._put(url, full_path, headers).json()
        return {'version': res.get('version', ''),
                'size': int(res['size'])}

    def download_raw_files(self, metadata: list, dir_name: str) -> Tuple[list, list]:
        """Download raw files."""
        urls = [path.join(self._url, row['s3path'][1:]) for row in metadata]
        full_paths = [path.join(dir_name, row['filename']) for row in metadata]
        for args in zip(urls, full_paths):
            self._get(*args)
        uuids = [row['uuid'] for row in metadata]
        return full_paths, uuids

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
                                 product: str,
                                 model: Optional[str] = None,
                                 legacy: Optional[bool] = False) -> list:
        """Create and upload images."""
        temp_file = NamedTemporaryFile(suffix='.png')
        try:
            if product in utils.get_product_types(level='3'):
                fields = utils.get_fields_for_l3_plot(product, model)
            else:
                fields, max_alt = utils.get_fields_for_plot(product)
        except NotImplementedError:
            logging.warning(f'Plotting for {product} not implemented')
            return []
        visualizations = []
        for field in fields:
            try:
                if legacy:
                    generate_legacy_figure(nc_file_full_path, product, field,
                                           image_name=temp_file.name, max_y=max_alt, dpi=120)
                if product in utils.get_product_types(level='3'):
                    l3_product = utils.full_product_to_l3_product(product)
                    generate_L3_day_plots(nc_file_full_path, l3_product, model, [field],
                                          image_name=temp_file.name,
                                          fig_type='single',
                                          title=False)
                else:
                    generate_figure(nc_file_full_path, [field], show=False,
                                    image_name=temp_file.name, max_y=max_alt, sub_title=False,
                                    title=False, dpi=120)
            except (IndexError, ValueError, TypeError) as err:
                logging.warning(err)
                continue
            s3key = product_key.replace('.nc', f"-{uuid[:8]}-{field}.png")
            url = path.join(self._url, 'cloudnet-img', s3key)
            headers = self._get_headers(temp_file.name)
            self._put(url, temp_file.name, headers=headers)
            visualizations.append({
                's3key': s3key,
                'variable_id': utils.get_var_id(product, field),
            })
        return visualizations

    def _put(self, url: str, full_path: str, headers: Optional[dict] = None) -> requests.Response:
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
