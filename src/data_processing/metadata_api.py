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
        self._url = config['METADATASERVER']['url']

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

    def find_volatile_files_to_freeze(self) -> list:
        """Find volatile files released before certain time limit."""
        freeze_after = {key: int(value) for key, value in self.config['FREEZE_AFTER'].items()}
        updated_before = datetime.now() - timedelta(**freeze_after)
        payload = {
            'volatile': True,
            'releasedBefore': updated_before
        }
        return self.get('api/files', payload)

    def screen_metadata(self, metadata: list, instrument: str = None, product: str = None) -> list:
        """Return metadata suitable for processing."""
        if product:
            metadata = self._select_product(metadata, product)
        elif instrument:
            metadata = self._select_instrument(metadata, instrument)
            if instrument == 'hatpro':
                metadata = self._select_lwp(metadata)
        else:
            metadata = self._select_optimum_model(metadata)
        return metadata

    @staticmethod
    def _select_product(metadata: list, product: str) -> list:
        return [row for row in metadata if row['product']
                and row['product']['id'] == product]

    @staticmethod
    def _select_instrument(metadata: list, instrument: str) -> list:
        return [row for row in metadata if row['instrument']
                and row['instrument']['id'] == instrument]

    @staticmethod
    def _select_lwp(metadata: list) -> list:
        return [row for row in metadata if row['filename'].lower().endswith('.lwp.nc')]

    @staticmethod
    def _select_optimum_model(all_metadata_for_day: list) -> list:
        model_metadata = [row for row in all_metadata_for_day if row['model']]
        sorted_metadata = sorted(model_metadata, key=lambda k: k['model']['optimumOrder'])
        return [sorted_metadata[0]] if sorted_metadata else []
