"""Metadata API for Cloudnet files."""
import subprocess
from datetime import datetime, timedelta
from os import path
import requests


class MetadataApi:
    """Class handling connection between Cloudnet files and database."""

    def __init__(self, url, session=requests.Session()):
        self._url = url
        self._session = session

    def exists(self, uuid):
        """Check if given UUID exists in database."""
        url = path.join(self._url, 'api/files', uuid)
        res = self._session.get(url)
        return str(res.status_code) == '200'

    def put(self, uuid, filepath, freeze=False):
        """Put Cloudnet file to database."""
        payload = subprocess.check_output(['ncdump', '-xh', path.realpath(filepath)])
        url = path.join(self._url, 'files', uuid)
        headers = {'Content-Type': 'application/xml'}
        if freeze:
            headers['X-Freeze'] = 'True'
        res = self._session.put(url, data=payload, headers=headers)
        res.raise_for_status()
        return res

    def put_img(self, filepath, uuid, variable_id):
        """Put Cloudnet quicklook file to database."""
        basename = path.basename(filepath)
        url = path.join(self._url, 'visualizations', basename)
        payload = {
            'fullPath': filepath,
            'sourceFileId': uuid,
            'variableId': variable_id
        }
        res = self._session.put(url, json=payload)
        res.raise_for_status()
        return res

    def get_volatile_files_updated_before(self, **time_delta):
        """Find volatile files released before given time limit."""
        updated_before = datetime.now() - timedelta(**time_delta)
        url = path.join(self._url, 'api/files')
        payload = {
            'volatile': True,
            'releasedBefore': updated_before
        }
        res = self._session.get(url, params=payload)
        res.raise_for_status()
        return [file['filename'] for file in res.json()]

    def change_status_from_uploaded_to_processed(self, checksum: str):
        url = path.join(self._url, 'metadata', checksum)
        res = self._session.post(url, json={'status': 'processed'})
        res.raise_for_status()
        return res

    def get_uploaded_metadata(self, site: str, date_str: str, instrument: str) -> list:
        """Get uploaded metadata for certain site / date / instrument."""
        payload = {'dateFrom': date_str,
                   'dateTo': date_str,
                   'site': site,
                   'instrument': instrument}
        url = path.join(self._url, 'metadata')
        return requests.get(url, payload).json()
