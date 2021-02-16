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
    'METADATASERVER': {'url': mock_addr},
    'FREEZE_AFTER': {'days': 2}
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
        res = md_api.put('s3key', self.payload)
        assert res.text == 'resp'

    def test_raises_error_on_failed_request(self):
        adapter.register_uri('PUT', f'{mock_addr}files/s3key_fail', status_code=500)
        md_api = metadata_api.MetadataApi(config, session)
        with pytest.raises(requests.exceptions.HTTPError):
            md_api.put('s3key_fail', self.payload)

    def test_screen_metadata(self):
        metadata = [
            {'instrument': {'id': 'chm15k', 'type': 'lidar'},
             's3key': 'key1', 'filename': 'foo.nc'},
            {'instrument': {'id': 'chm15k', 'type': 'radar'},
             's3key': 'key2', 'filename': 'foo.nc'},
            {'instrument': {'id': 'hatpro', 'type': 'mwr'},
             's3key': 'key3', 'filename': 'foo.lwp.nc'},
            {'instrument': {'id': 'chm15k', 'type': 'hatpro'},
             's3key': 'key4', 'filename': 'foo.iwc.nc'},
            {'model': {'id': 'ecmwf', 'optimumOrder': '1'},
             's3key': 'key5', 'filename': 'foo.nc'},
            {'model': {'id': 'icon', 'optimumOrder': '0'},
             's3key': 'key6', 'filename': 'foo.nc'}
        ]
        md_api = metadata_api.MetadataApi(config, session)
        assert md_api.screen_metadata(metadata, model='ecmwf')[0]['s3key'] == 'key5'
        assert md_api.screen_metadata(metadata, model='icon')[0]['s3key'] == 'key6'
        meta = md_api.screen_metadata(metadata, instrument='chm15k')
        for row, key in zip(meta, ('key1', 'key2', 'key4')):
            assert key == row['s3key']
        assert md_api.screen_metadata(metadata, instrument='hatpro')[0]['s3key'] == 'key3'
        assert md_api.screen_metadata(metadata, instrument='xyz') == []
        assert md_api.screen_metadata(metadata, model='xyz') == []

    def test_calls_files_with_proper_params_and_parses_response_correctly(self):
        for end_point in ('files', 'model-files'):
            url = f'{mock_addr}api/{end_point}(.*?)'
            adapter.register_uri('GET', re.compile(url), json=json.loads(files_response))
        md_api = metadata_api.MetadataApi(config, session)
        r = md_api.find_volatile_files_to_freeze()
        assert len(r) == 2
        assert r[0]['filename'] == '20200513_granada_rpg-fmcw-94.nc'
