from xml.dom import minidom
import pytest
import requests
import requests_mock

metadata_api = __import__('operational-processing').metadata_api
adapter = requests_mock.Adapter()
session = requests.Session()
session.mount('mock://', adapter)

class TestMetadataApi:

    def test_put_metadata(self):
        def is_valid_xml(request):
            try:
                minidom.parseString(request.text)
            except (TypeError, AttributeError):
                return False
            return True

        adapter.register_uri('PUT', 'mock://test/file/uuid', additional_matcher=is_valid_xml, text='resp')
        md_api = metadata_api.MetadataApi('mock://test/', session)
        r = md_api.put('uuid', 'tests/data/output/bucharest/calibrated/chm15k/2020/20200118_bucharest_chm15k.nc')
            
        assert r.text == 'resp'
