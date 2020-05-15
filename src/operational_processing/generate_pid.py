import sys
import requests
from operational_processing.utils import str2bool


class PidGenerator:

    def __init__(self, options, session=requests.Session()):
        self.__options = options
        self.__session = self.__init_session(options, session)

    def __del__(self):
        self.__delete_session()

    def generate_pid(self, uuid):
        server_url = f'{self.__options["handle_server_url"]}api/handles/'
        prefix = self.__options['prefix']

        version = '1'
        uid = uuid[:16]
        suffix = f'{version}.{uid}'

        handle = f'{prefix}/{suffix}'
        target = f'{self.__options["resolve_to_url"]}{uuid}'

        r = self.__session.put(f'{server_url}{handle}',
            json=self.__get_payload(target))
        r.raise_for_status()

        if r.status_code == 200:
            print(f'WARN: Handle {handle} already exists, updating handle.', file=sys.stderr)

        return f'https://hdl.handle.net/{r.json()["handle"]}' 
    
    def __init_session(self, options, session):
        session.verify = str2bool(options['ca_verify'])
        session.headers['Content-Type'] = 'application/json'

        # Authenticate session
        session_url = f'{options["handle_server_url"]}api/sessions'
        session.headers['Authorization'] = f'Handle clientCert="true"'
        cert = (str2bool(options['certificate_only']), str2bool(options['private_key']))
        r = session.post(session_url, cert=cert)
        r.raise_for_status()
        session_id = r.json()['sessionId']
        session.headers['Authorization'] = f'Handle sessionId={session_id}'

        return session

    def __delete_session(self):
        session_url = f'{self.__options["handle_server_url"]}api/sessions/this'
        self.__session.delete(session_url)
        self.__session.close()

    def __get_payload(self, target):
        return {
            'values': [{
                'index': 1,
                'type': 'URL',
                'data': {
                    'format': 'string',
                    'value': target
                }
            }, {
                'index': 100,
                'type': 'HS_ADMIN',
                'data': {
                    'format': 'admin',
                    'value': {
                        'handle': f'0.NA/{self.__options["prefix"]}',
                        'index': 200,
                        'permissions': '011111110011'
                    }
                }
            }
        ]}
