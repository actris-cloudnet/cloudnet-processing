import logging
import re

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from data_processing import metadata_api
from data_processing.utils import read_main_conf
from housekeeping import HousekeepingEmptyWarning
from housekeeping import get_config as get_housekeeping_config
from housekeeping import hatprohkd2db, nc2db, rpg2db


def main(args):
    cfg_main = read_main_conf()
    cfg_hk = get_housekeeping_config()
    instruments = cfg_hk["instruments"]
    md_api = metadata_api.MetadataApi(cfg_main, _http_session())
    metadata = md_api.get(
        "api/raw-files",
        {
            "site": args.site,
            "instrument": instruments,
            "dateFrom": args.start,
            "dateTo": args.stop,
            "status": ["uploaded", "processed"],
        },
    )
    re_nc = re.compile(r"^.+\.nc$", re.I)
    re_hkd = re.compile(r"^.+\.hkd$", re.I)
    re_rpg = re.compile(r"^.+\.LV1$", re.I)
    raw_api = RawApi(cfg_main)
    for record in metadata:
        fname = record["filename"]
        uuid = record["uuid"]

        try:
            if re_nc.match(fname):
                logging.info(f"Processing housekeeping data: {fname}")
                filebytes = raw_api.get_raw_file(uuid, fname)
                nc2db(filebytes, record)
            elif re_hkd.match(fname):
                logging.info(f"Processing housekeeping data: {fname}")
                filebytes = raw_api.get_raw_file(uuid, fname)
                hatprohkd2db(filebytes, record)
            elif re_rpg.match(fname) and record["instrumentId"] == "rpg-fmcw-94":
                logging.info(f"Processing housekeeping data: {fname}")
                filebytes = raw_api.get_raw_file(uuid, fname)
                rpg2db(filebytes, record)
            else:
                logging.info(f"Skipping: {fname}")
        except HousekeepingEmptyWarning:
            logging.warning(f"No housekeeping data found: {fname}")


class RawApi:
    def __init__(self, cfg):
        self.base_url = cfg["DATAPORTAL_URL"]
        self.session = _http_session()

    def get_raw_file(self, uuid: str, fname: str) -> bytes:
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
    subparser.add_parser("housekeeping", help="Process housekeeping data")
    return subparser
