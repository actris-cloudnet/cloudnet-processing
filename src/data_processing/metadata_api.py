"""Metadata API for Cloudnet files."""
from argparse import Namespace
from datetime import timedelta
from typing import Union, Optional
import logging
import os
import requests
from data_processing import utils


class MetadataApi:
    """Class handling connection between Cloudnet files and database."""

    def __init__(self, config: dict, session: requests.Session):
        self.config = config
        self.session = session
        self._url = config['DATAPORTAL_URL']

    def get(self,
            end_point: str,
            payload: Optional[dict] = None) -> Union[list, dict]:
        """Get Cloudnet metadata."""
        url = os.path.join(self._url, end_point)
        res = self.session.get(url, params=payload)
        res.raise_for_status()
        return res.json()

    def post(self,
             end_point: str,
             payload: dict,
             auth: Optional[tuple] = None) -> requests.Response:
        """Update upload / product metadata."""
        url = os.path.join(self._url, end_point)
        res = self.session.post(url, json=payload, auth=auth)
        res.raise_for_status()
        return res

    def put(self,
            end_point: str,
            resource: str,
            payload: dict) -> requests.Response:
        """PUT metadata to Cloudnet data portal."""
        url = os.path.join(self._url, end_point, resource)
        res = self.session.put(url, json=payload)
        res.raise_for_status()
        return res

    def put_file(self,
                 end_point: str,
                 resource: str,
                 full_path: str,
                 auth: tuple) -> requests.Response:
        """PUT file to Cloudnet data portal."""
        url = os.path.join(self._url, end_point, resource)
        res = requests.put(url, data=open(full_path, 'rb'), auth=auth)
        res.raise_for_status()
        return res

    def put_images(self,
                   img_metadata: list,
                   product_uuid: str):
        for data in img_metadata:
            payload = {
                'sourceFileId': product_uuid,
                'variableId': data['variable_id']
            }
            self.put('visualizations', data['s3key'], payload)

    def upload_instrument_file(self,
                               full_path: str,
                               instrument: str,
                               date: str,
                               site: str,
                               filename: Optional[str] = None):
        auth = (site, 'letmein')
        checksum = utils.md5sum(full_path)
        metadata = {
            'filename': filename or os.path.basename(full_path),
            'checksum': checksum,
            'instrument': instrument,
            'measurementDate': date
        }
        try:
            self.post('upload/metadata', metadata, auth=auth)
        except requests.exceptions.HTTPError as err:
            logging.info(err)
            return
        self.put_file('upload/data', checksum, full_path, auth)

    def find_files_to_freeze(self, args: Namespace) -> list:
        """Find volatile files released before certain time limit."""
        freeze_model_files = not args.products or 'model' in args.products
        products = _get_regular_products(args)
        common_payload = _get_common_payload(args)

        # Regular files
        files_payload = {
            **common_payload,
            **{'product': products},
            **self._get_freeze_payload('FREEZE_AFTER_DAYS', args)
        }
        regular_files = self.get('api/files', files_payload)

        # Model files
        model_files = []
        if freeze_model_files:
            models_payload = {
                **common_payload,
                **{'allModels': True},
                **self._get_freeze_payload('FREEZE_MODEL_AFTER_DAYS', args)
            }
            model_files = self.get('api/model-files', models_payload)

        return regular_files + model_files

    def _get_freeze_payload(self, key: str, args: Namespace) -> dict:
        freeze_after_days = int(self.config[key])
        updated_before = (utils.get_helsinki_datetime() - timedelta(days=freeze_after_days)).isoformat()
        if args.force:
            logging.warning(f'Overriding {key} -config option. Also recently changed files may be freezed.')
            updated_before = None
        return {
            'volatile': True,
            'releasedBefore': updated_before
        }

    def find_files_for_plotting(self, args: Namespace) -> list:
        common_payload = _get_common_payload(args)
        products = _get_regular_products(args)
        files_payload = {
            **common_payload,
            **{'product': products},
        }
        files = self.get('api/files', files_payload)
        return files


def _get_common_payload(args: Namespace) -> dict:
    return {
        'site': args.sites,
        'dateFrom': args.start,
        'dateTo': args.stop,
        'date': args.date
    }


def _get_regular_products(args: Namespace) -> list:
    if args.products:
        return [prod for prod in args.products if prod != 'model']
