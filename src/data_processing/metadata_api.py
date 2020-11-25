"""Metadata API for Cloudnet files."""
from datetime import date, timedelta, datetime
from os import path
import requests


class MetadataApi:
    """Class handling connection between Cloudnet files and database."""

    def __init__(self, url, session=requests.Session()):
        self.url = url
        self.session = session
        self._end_point = {
            'upload': 'upload-metadata',
            'product': 'files',
            'public': 'api/files',
            'visualizations': 'visualizations'
        }

    def put(self, s3key: str, payload: dict) -> requests.Response:
        """Add Cloudnet product metadata."""
        url = path.join(self.url, self._end_point['product'], s3key)
        res = self.session.put(url, json=payload)
        res.raise_for_status()
        return res

    def put_img(self, data: dict, uuid: str) -> requests.Response:
        """Put Cloudnet quicklook file to database."""
        url = path.join(self.url, self._end_point['visualizations'], data['s3key'])
        payload = {
            'sourceFileId': uuid,
            'variableId': data['variable_id']
        }
        res = self.session.put(url, json=payload)
        res.raise_for_status()
        return res

    def post(self, end_point: str, payload: dict) -> requests.Response:
        """Update upload / product metadata."""
        assert end_point in (self._end_point['product'], self._end_point['upload'])
        url = path.join(self.url, end_point)
        res = self.session.post(url, json=payload)
        res.raise_for_status()
        return res

    def get_volatile_files_updated_before(self, **time_delta: dict) -> list:
        """Find volatile files released before given time limit."""
        updated_before = datetime.now() - timedelta(**time_delta)
        url = path.join(self.url, self._end_point['public'])
        payload = {
            'volatile': True,
            'releasedBefore': updated_before
        }
        res = self.session.get(url, params=payload)
        res.raise_for_status()
        return [file['s3key'] for file in res.json()]

    def get_uploaded_metadata(self, site: str, date_str: str) -> list:
        """Get uploaded metadata for certain site / date / instrument."""
        payload = {
            'dateFrom': date_str,
            'dateTo': date_str,
            'site': site
        }
        url = path.join(self.url, self._end_point['upload'])
        res = requests.get(url, payload)
        res.raise_for_status()
        return res.json()

    def screen_metadata(self, metadata: list, new_version: bool, instrument: str = None) -> list:
        """Return metadata suitable for processing."""
        if instrument:
            metadata = self._select_instrument_files(metadata, instrument)
            if instrument == 'hatpro':
                metadata = self._select_lwp_files_only(metadata)
        else:
            metadata = self._select_optimum_model(metadata)
        self._raise_if_nothing_to_process(metadata, new_version)
        return metadata

    def exists(self, uuid):
        """Check if given UUID exists in database."""
        url = path.join(self.url, self._end_point['public'], uuid)
        res = self.session.get(url)
        return str(res.status_code) == '200'

    @staticmethod
    def _select_instrument_files(metadata: list, instrument: str):
        return [row for row in metadata if row['instrument'] and row['instrument']['id'] == instrument]

    @staticmethod
    def _select_lwp_files_only(metadata: list) -> list:
        return [row for row in metadata if row['filename'].lower().endswith('.lwp.nc')]

    @staticmethod
    def _select_optimum_model(all_metadata_for_day: list) -> list:
        model_metadata = [row for row in all_metadata_for_day if row['model']]
        sorted_metadata = sorted(model_metadata, key=lambda k: k['model']['optimumOrder'])
        return [sorted_metadata[0]] if sorted_metadata else []

    @staticmethod
    def _raise_if_nothing_to_process(metadata: list, new_version: bool) -> None:
        is_unprocessed_data = any([row['status'] == 'uploaded' for row in metadata])
        if (not new_version and not is_unprocessed_data) or not metadata:
            raise ValueError
