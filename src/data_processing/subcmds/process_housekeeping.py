import requests
from housekeeping import nc2db
from housekeeping import get_config as get_housekeeping_config
from pdb import set_trace as db

import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from typing import Optional
from tempfile import TemporaryDirectory

from data_processing import metadata_api, utils
from data_processing.processing_tools import ProcessBase
from data_processing.storage_api import StorageApi
from data_processing.utils import make_session, read_main_conf

def main(args, session: Optional[requests.Session] = None):
    if session is None:
        session = make_session()
    cfg_main = read_main_conf()
    cfg_hk = get_housekeeping_config()
    instruments = cfg_hk["instruments"]
    md_api = metadata_api.MetadataApi(cfg_main, make_session())
    metadata = md_api.get("api/raw-files",{
        "site": args.site,
        "instrument": instruments,
        "dateFrom": args.start,
        "dateTo": args.stop,
        })
    re_nc = re.compile(r"^.+\.nc$")
    raw_api = RawApi(cfg_main)
    for m in metadata:
        fname = m["filename"]
        url = m["downloadUrl"]
        uuid = m["uuid"]
        site_id = m["siteId"]
        instrument_id = m["instrumentId"]
        instrument_pid = m["instrumentPid"]
        if re_nc.match(fname):
            nc_bytes = raw_api.get_raw_file(uuid,fname)
            nc2db(nc_bytes, site_id, instrument_id, instrument_pid)

class RawApi:
    def __init__(self, cfg):
        self.base_url = cfg["DATAPORTAL_URL"]
        self.session = _http_session()
    def get_raw_file(self, uuid: str,fname: str) -> bytes:
        url = f"{self.base_url}api/download/raw/{uuid}/{fname}"
        return self.session.get(url).content

def _http_session():
    retries = Retry(total=10, backoff_factor=0.2)
    adapter = HTTPAdapter(max_retries=retries)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def add_arguments(subparser):
    parser = subparser.add_parser("housekeeping", help="Process housekeeping data")
    return subparser

