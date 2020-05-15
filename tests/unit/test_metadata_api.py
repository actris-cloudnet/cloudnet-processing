from xml.dom import minidom
import pytest
import requests
import requests_mock

metadata_api = __import__('operational_processing').metadata_api
adapter = requests_mock.Adapter()
session = requests.Session()
session.mount('mock://', adapter)
mock_addr = 'mock://test/'


class TestMetadataApi:

    def test_put_metadata(self):
        adapter.register_uri('PUT', f'{mock_addr}file/uuid', additional_matcher=self.__is_valid_xml, text='resp')
        md_api = metadata_api.MetadataApi(mock_addr, session)
        r = md_api.put('uuid', 'tests/data/output_fixed/bucharest/calibrated/chm15k/2020/20200118_bucharest_chm15k.nc')
            
        assert r.text == 'resp'

    def test_put_metadata_freeze(self):
        def has_freeze_in_header(request):
            if not self.__is_valid_xml(request):
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

    def __is_valid_xml(self, request):
        try:
            minidom.parseString(request.text)
        except (TypeError, AttributeError):
            return False
        return True
