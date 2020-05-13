import subprocess
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
