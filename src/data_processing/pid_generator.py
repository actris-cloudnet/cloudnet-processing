"""Module containing classes / functions for PID generation."""
import sys
import requests
from typing import Tuple
import netCDF4
from data_processing.utils import str2bool


class PidGenerator:
    """The PidGenerator class."""

    def __init__(self, options, session=requests.Session()):
        self._options = options
        self._session = self._init_session(options, session)

    def __del__(self):
        if hasattr(self, '_session'):
            session_url = f'{self._options["handle_server_url"]}api/sessions/this'
            self._session.delete(session_url)
            self._session.close()

    def generate_pid(self, uuid):
        """Generates PID from given UUID."""
        server_url = f'{self._options["handle_server_url"]}api/handles/'
        prefix = self._options['prefix']

        version = '1'
        uid = uuid[:16]
        suffix = f'{version}.{uid}'

        handle = f'{prefix}/{suffix}'
        target = f'{self._options["resolve_to_url"]}{uuid}'

        res = self._session.put(f'{server_url}{handle}',
                                json=self._get_payload(target))
        res.raise_for_status()

        if res.status_code == 200:
            print(f'WARN: Handle {handle} already exists, updating handle.', file=sys.stderr)

        return f'https://hdl.handle.net/{res.json()["handle"]}'

    @staticmethod
    def _init_session(options, session):
        session.verify = str2bool(options['ca_verify'])
        session.headers['Content-Type'] = 'application/json'

        # Authenticate session
        session_url = f'{options["handle_server_url"]}api/sessions'
        session.headers['Authorization'] = 'Handle clientCert="true"'
        cert = (str2bool(options['certificate_only']), str2bool(options['private_key']))
        res = session.post(session_url, cert=cert)
        res.raise_for_status()
        session_id = res.json()['sessionId']
        session.headers['Authorization'] = f'Handle sessionId={session_id}'

        return session

    def _get_payload(self, target):
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
                        'handle': f'0.NA/{self._options["prefix"]}',
                        'index': 200,
                        'permissions': '011111110011'
                    }
                }
            }]
        }


def add_pid_to_file(pid_gen: PidGenerator, filename: str) -> Tuple[str, str]:
    """Generates PID and adds it to nc-global attributes."""
    root_grp = netCDF4.Dataset(filename, 'r+')
    uuid = getattr(root_grp, 'file_uuid')
    pid = pid_gen.generate_pid(uuid)
    root_grp.pid = pid
    root_grp.close()
    return pid, uuid
