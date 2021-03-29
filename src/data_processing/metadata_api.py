"""Metadata API for Cloudnet files."""
from datetime import timedelta, datetime
from typing import Union
from os import path
import requests


class MetadataApi:
    """Class handling connection between Cloudnet files and database."""

    def __init__(self, config: dict, session=requests.Session()):
        self.config = config
        self.session = session
        self._url = config['DATAPORTAL_URL']

    def get(self, end_point: str, payload: dict) -> Union[list, dict]:
        """Get Cloudnet metadata."""
        url = path.join(self._url, end_point)
        res = self.session.get(url, params=payload)
        res.raise_for_status()
        return res.json()

    def put(self, s3key: str, payload: dict) -> requests.Response:
        """Add Cloudnet product metadata."""
        url = path.join(self._url, 'files', s3key)
        res = self.session.put(url, json=payload)
        res.raise_for_status()
        return res

    def post(self, end_point: str, payload: dict) -> requests.Response:
        """Update upload / product metadata."""
        url = path.join(self._url, end_point)
        res = self.session.post(url, json=payload)
        res.raise_for_status()
        return res

    def put_img(self, data: dict, uuid: str) -> requests.Response:
        """Put Cloudnet quicklook file to database."""
        url = path.join(self._url, 'visualizations', data['s3key'])
        payload = {
            'sourceFileId': uuid,
            'variableId': data['variable_id']
        }
        res = self.session.put(url, json=payload)
        res.raise_for_status()
        return res

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
        freeze_after_days = self.config[key]
        return datetime.now() - timedelta(days=freeze_after_days)

    @staticmethod
    def _get_freeze_payload(updated_before: datetime) -> dict:
        return {
            'volatile': True,
            'releasedBefore': updated_before
        }
