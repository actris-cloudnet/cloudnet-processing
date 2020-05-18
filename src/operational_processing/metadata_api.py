import subprocess
from datetime import date, timedelta
import requests


class MetadataApi:

    def __init__(self, url, session=requests.Session()):
        self.__url = url
        self.__session = session

    def put(self, uuid, filepath, freeze=False):
        payload = subprocess.check_output(['ncdump', '-xh', filepath])
        url = f'{self.__url}file/{uuid}'
        headers = { 'Content-Type': 'application/xml' }
        if freeze:
            headers['X-Freeze'] = 'True'
        r = self.__session.put(url, data=payload, headers=headers)
        r.raise_for_status()
        return r

    def get_volatile_files_updated_before(self, **time_delta):
        not_updated_before = date.today() - timedelta(**time_delta)

        url = f'{self.__url}files'
        payload = {
            'volatile': True,
            'releasedAtAfter': not_updated_before
        }
        r = self.__session.get(url, params=payload)
        r.raise_for_status()

        return [file['filename'] for file in r.json()]
