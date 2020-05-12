import subprocess
import requests

class MetadataApi:

    def __init__(self, url, session=requests.Session()):
        self.__url = url
        self.__session = session

    def put(self, uuid, filepath):
        payload = subprocess.check_output(['ncdump', '-xh', filepath])
        url = f'{self.__url}file/{uuid}'
        r = self.__session.put(url, data=payload)
        r.raise_for_status()
        return r
