"""Metadata API for Cloudnet files."""
from datetime import timedelta, datetime
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

    def find_volatile_regular_files_to_freeze(self) -> list:
        """Find volatile files released before certain time limit."""
        updated_before = self._get_freeze_limit('FREEZE_AFTER_DAYS')
        payload = self._get_freeze_payload(updated_before)
        return self.get('api/files', payload)

    def find_volatile_model_files_to_freeze(self) -> list:
        """Find volatile model files released before certain time limit."""
        updated_before = self._get_freeze_limit('FREEZE_MODEL_AFTER_DAYS')
        payload = self._get_freeze_payload(updated_before)
        payload['allModels'] = True
        return self.get('api/model-files', payload)

    def _get_freeze_limit(self, key: str) -> datetime:
        freeze_after_days = int(self.config[key])
        return utils.get_helsinki_datetime() - timedelta(days=freeze_after_days)

    @staticmethod
    def _get_freeze_payload(updated_before: datetime) -> dict:
        return {
            'volatile': True,
            'releasedBefore': updated_before
        }

