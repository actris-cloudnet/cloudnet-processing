import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import housekeeping
from data_processing import metadata_api
from data_processing.utils import read_main_conf


def main(args):
    cfg_main = read_main_conf()
    instruments = housekeeping.list_instruments()
    md_api = metadata_api.MetadataApi(cfg_main, _http_session())
    query_params = {
        "site": args.site,
        "instrument": instruments,
        "status": ["uploaded", "processed"],
    }
    if args.date is not None:
        query_params.update({"date": args.date})
    else:
        query_params.update(
            {
                "dateFrom": args.start,
                "dateTo": args.stop,
            }
        )
    metadata = md_api.get(
        "api/raw-files",
        query_params,
    )
    raw_api = RawApi(cfg_main)
    for record in metadata:
        fname = record["filename"]
        uuid = record["uuid"]

        reader = housekeeping.get_reader(record)
        if reader is None:
            logging.info(f"Skipping: {fname}")
            continue

        logging.info(f"Processing housekeeping data: {fname}")
        filebytes = raw_api.get_raw_file(uuid, fname)
        try:
            df = reader(filebytes)
            if df.empty:
                logging.warning("Unable to process file: No housekeeping data found")
                continue
            housekeeping.write(df, record)
        except housekeeping.UnsupportedFile as e:
            logging.warning(f"Unable to process file: {e}")


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
