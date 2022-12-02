import logging

import requests

import housekeeping
from data_processing.metadata_api import MetadataApi
from data_processing.utils import make_session, read_main_conf


def main(args):
    cfg_main = read_main_conf()
    session = make_session()
    md_api = MetadataApi(cfg_main, session)
    raw_api = RawApi(cfg_main, session)

    instruments = housekeeping.list_instruments()
    query_params = {
        "site": args.site,
        "instrument": instruments,
        "status": ["uploaded", "processed"],
    }
    if args.date is not None:
        query_params["date"] = args.date
    else:
        query_params["dateFrom"] = args.start
        query_params["dateTo"] = args.stop
    metadata = md_api.get(
        "api/raw-files",
        query_params,
    )
    with housekeeping.Database() as db:
        for record in metadata:
            filename = record["filename"]
            uuid = record["uuid"]

            reader = housekeeping.get_reader(record)
            if reader is None:
                logging.info(f"Skipping: {filename}")
                continue

            logging.info(f"Processing housekeeping data: {filename}")
            filebytes = raw_api.get_raw_file(uuid, filename)
            try:
                points = reader(filebytes, record)
                db.write(points)
            except housekeeping.UnsupportedFile as e:
                logging.warning(f"Unable to process file: {e}")


class RawApi:
    def __init__(self, cfg: dict, session: requests.Session):
        self.base_url = cfg["DATAPORTAL_URL"]
        self.session = session

    def get_raw_file(self, uuid: str, fname: str) -> bytes:
        url = f"{self.base_url}api/download/raw/{uuid}/{fname}"
        return self.session.get(url).content


def add_arguments(subparser):
    subparser.add_parser("housekeeping", help="Process housekeeping data")
    return subparser
