import json
import pytest
import requests
import requests_mock
from data_processing import metadata_api
import re

adapter = requests_mock.Adapter()
session = requests.Session()
session.mount('http://', adapter)
mock_addr = 'http://test/'
config = {
    'DATAPORTAL_URL': mock_addr,
    'FREEZE_AFTER_DAYS': 2,
    'FREEZE_MODEL_AFTER_DAYS': 3
}

files_response = '''
[
  {
    "uuid": "42d523cc-764f-4334-aefc-35a9ca71f342",
    "title": "Radar file from Granada",
    "measurementDate": "2020-05-13",
    "history": "2020-05-15 05:00:26 - radar file created",
    "publicity": "public",
    "cloudnetpyVersion": "1.0.9",
    "releasedAt": "2020-05-15T05:00:27.538Z",
    "filename": "20200513_granada_rpg-fmcw-94.nc",
    "checksum": "33727ffe97e96baef0e4c6708a14ceb3b18fbe32ebeef28efe8703434fcceaaa",
    "size": 19883377,
    "format": "HDF5 (NetCDF4)",
    "site": {
      "id": "granada",
      "humanReadableName": "Granada",
      "latitude": 37.164,
      "longitude": -3.605,
      "altitude": 680,
      "gaw": "UGR",
      "country": "Spain",
      "isTestSite": false
    },
    "product": {
      "id": "radar",
      "humanReadableName": "Radar",
      "level": "1"
    },
    "volatile": true,
    "url": "https://altocumulus.fmi.fi/download/20200513_granada_rpg-fmcw-94.nc"
  }
]
'''


class TestMetadataApi:

    payload = {
        'product': 'model',
        'site': 'bucharest',
        'measurementDate': '2020-10-21',
        'format': 'NetCDF3',
        'checksum': '23cf03694943fbcd1cce9ff45ea05242fae68cda06f127668963bb99d0b2de33',
        'volatile': True,
        'uuid': 'b09174d204e04ffbb6d284c150bb7271',
        'pid': '',
        'history': "Some arbitrary history",
        'cloudnetpyVersion': '',
        'version': '',
        'size': 501524
    }

    def test_put_metadata(self):
        adapter.register_uri('PUT', f'{mock_addr}files/s3key', text='resp')
        md_api = metadata_api.MetadataApi(config, session)
        res = md_api.put('files', 's3key', self.payload)
        assert res.text == 'resp'

    def test_raises_error_on_failed_request(self):
        adapter.register_uri('PUT', f'{mock_addr}files/s3key_fail', status_code=500)
        md_api = metadata_api.MetadataApi(config, session)
        with pytest.raises(requests.exceptions.HTTPError):
            md_api.put('files', 's3key_fail', self.payload)

    def test_calls_files_with_proper_params_and_parses_response_correctly(self):
        for end_point in ('files', 'model-files'):
            url = f'{mock_addr}api/{end_point}(.*?)'
            adapter.register_uri('GET', re.compile(url), json=json.loads(files_response))
        md_api = metadata_api.MetadataApi(config, session)
        regular_files = md_api.find_volatile_regular_files_to_freeze()
        model_files = md_api.find_volatile_regular_files_to_freeze()
        assert len(regular_files + model_files) == 2
        assert regular_files[0]['filename'] == '20200513_granada_rpg-fmcw-94.nc'
