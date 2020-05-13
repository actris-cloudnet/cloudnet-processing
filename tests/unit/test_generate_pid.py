import pytest
import requests
import requests_mock

generate_pid = __import__('operational-processing').generate_pid
adapter = requests_mock.Adapter()
session = requests.Session()
session.mount('mock://', adapter)

options = dict(
    handle_server_url = 'mock://test/',
    prefix = '21.T12995',
    certificate_only = None,
    private_key = None,
    ca_verify = 'False',
    resolve_to_url = 'mock://test2/'
)

class TestMetadataApi:

    def test_generate_pid(self):
        sess_response = {'sessionId': 'fnoskvnqmxlc8ihllcl566sk', 'nonce': '3sBJALp5eXKWVB9jSDtnGQ==', 'authenticated': True, 'id': '309:21.T12995/USER01'}
        handle_response = {'responseCode': 1, 'handle': '21.T12995/1.be8154c1a6aa4f44'}
        adapter.register_uri('POST', 'mock://test/api/sessions', json=sess_response)
        adapter.register_uri('DELETE', 'mock://test/api/sessions/this')
        adapter.register_uri('PUT', 'mock://test/api/handles/21.T12995/1.be8154c1a6aa4f44', additional_matcher=self.__is_valid_json, json=handle_response)

        pid_gen = generate_pid.PidGenerator(options, session=session)
        pid = pid_gen.generate_pid('be8154c1a6aa4f44b953780b016987b5')
            
        assert pid == 'https://hdl.handle.net/21.T12995/1.be8154c1a6aa4f44'

    def test_raises_error_on_failed_request(self):
        adapter.register_uri('PUT', 'mock://test/api/handles/21.T12995/1.fail', status_code=403)

        pid_gen = generate_pid.PidGenerator(options, session=session)

        with pytest.raises(requests.exceptions.HTTPError):
            pid_gen.generate_pid('fail')

    def __is_valid_json(self, request):
        try:
            request.json()
        except ValueError:
            return False
        return True