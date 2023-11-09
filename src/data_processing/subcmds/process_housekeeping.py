import housekeeping

from data_processing.metadata_api import MetadataApi
from data_processing.utils import RawApi, make_session, read_main_conf


def main(args):
    cfg_main = read_main_conf()
    session = make_session()
    md_api = MetadataApi(cfg_main, session)
    raw_api = RawApi(session=session)

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
            housekeeping.process_record(record, raw_api, db)


def add_arguments(subparser):
    subparser.add_parser("housekeeping", help="Process housekeeping data")
    return subparser
