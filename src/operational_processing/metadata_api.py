import subprocess
from datetime import date, timedelta
from os import path

import requests


class MetadataApi:

    def __init__(self, url, session=requests.Session()):
        self._url = url
        self._session = session

    def put(self, uuid, filepath, freeze=False):
        payload = subprocess.check_output(['ncdump', '-xh', path.realpath(filepath)])
        url = f'{self._url}file/{uuid}'
        headers = { 'Content-Type': 'application/xml' }
        if freeze:
            headers['X-Freeze'] = 'True'
        r = self._session.put(url, data=payload, headers=headers)
        r.raise_for_status()
        return r

    def get_volatile_files_updated_before(self, **time_delta):
        updated_before = date.today() - timedelta(**time_delta)

        url = f'{self._url}files'
        payload = {
            'volatile': True,
            'releasedBefore': updated_before
        }
        r = self._session.get(url, params=payload)
        r.raise_for_status()

        return [file['filename'] for file in r.json()]
