import json
from xml.dom import minidom
from datetime import date, timedelta

import pytest
import requests
import requests_mock

from operational_processing import metadata_api

adapter = requests_mock.Adapter()
session = requests.Session()
session.mount('http://', adapter)
mock_addr = 'http://test/'

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

    def test_put_metadata(self):
        adapter.register_uri('PUT', f'{mock_addr}file/uuid', additional_matcher=is_valid_xml, text='resp')
        md_api = metadata_api.MetadataApi(mock_addr, session)
        r = md_api.put('uuid', 'tests/data/output_fixed/bucharest/calibrated/chm15k/2020/20200118_bucharest_chm15k.nc')
            
        assert r.text == 'resp'

    def test_put_metadata_freeze(self):
        def has_freeze_in_header(request):
            if not is_valid_xml(request):
                return False
            return 'X-Freeze' in request.headers

        adapter.register_uri('PUT', f'{mock_addr}file/uuid', additional_matcher=has_freeze_in_header, text='resp')
        md_api = metadata_api.MetadataApi(mock_addr, session)
        r = md_api.put('uuid', 'tests/data/output_fixed/bucharest/calibrated/chm15k/2020/20200118_bucharest_chm15k.nc',
                       freeze=True)
            
        assert r.text == 'resp'

    def test_raises_error_on_failed_request(self):
        adapter.register_uri('PUT', f'{mock_addr}file/uuid_fail', status_code=500)

        md_api = metadata_api.MetadataApi(mock_addr, session)

        with pytest.raises(requests.exceptions.HTTPError):
            md_api.put('uuid_fail',
                       'tests/data/output_fixed/bucharest/calibrated/chm15k/2020/20200118_bucharest_chm15k.nc')

    def test_calls_files_with_proper_params_and_parses_response_correctly(self):
        now = date.today()
        two_days_ago = now - timedelta(days=2)

        adapter.register_uri('GET', f'http://test/files?volatile=True&releasedBefore={two_days_ago}',
                             json=json.loads(files_response))

        md_api = metadata_api.MetadataApi(mock_addr, session)
        r = md_api.get_volatile_files_updated_before(days=2)

        assert len(r) == 1
        assert r[0] == '20200513_granada_rpg-fmcw-94.nc'


def is_valid_xml(request):
    try:
        minidom.parseString(request.text)
    except (TypeError, AttributeError):
        return False
    return True
