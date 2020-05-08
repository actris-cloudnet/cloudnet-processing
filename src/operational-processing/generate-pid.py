from pyhandle.handleclient import PyHandleClient
from pyhandle.clientcredentials import PIDClientCredentials
process_utils = importlib.import_module("operational-processing").utils
import uuid

class PidGenerator:

    def __init__(self, options):
        self.__options = options
        self.__client = PyHandleClient('rest').instantiate_with_credentials(self.__options)

    def generate_pid(self, uuid):
        prefix = self.__options['prefix']

        version = '1'
        uid = uuid[:16]
        suffix = f'{version}.{uid}'

        handle = f'{prefix}/{suffix}'
        pid = self.__client.register_handle(handle, f'https://altocumulus.fmi.fi/file/{uid}')

        return f'https://hdl.handle.net/{pid}' 
