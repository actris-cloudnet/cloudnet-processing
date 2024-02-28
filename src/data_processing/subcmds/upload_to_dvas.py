#!/usr/bin/env python3
import logging

from data_processing import utils
from data_processing.dvas import Dvas, DvasError
from data_processing.metadata_api import MetadataApi


def main(args):
    config = utils.read_main_conf()
    md_api = MetadataApi(config)

    if args.truncate:
        _truncate_clu_data(md_api)
        return

    files = _get_files(md_api, args)

    if args.delete:
        _delete_clu_data(md_api, files)
    else:
        dvas = Dvas()
        logging.info(f"Uploading {len(files)} CLU files to the DVAS data portal.")
        for file in files:
            dvas.upload(md_api, file)


def _get_files(md_api: MetadataApi, args) -> list[dict]:
    payload = {
        "product": args.products,
        "site": args.site,
        "dateFrom": args.date if args.date is not None else args.start,
        "dateTo": args.date if args.date is not None else args.stop,
        "dvasUpdated": True if args.delete else False,
    }
    files = md_api.get("api/files", payload)
    if "model" in args.products:
        del payload["product"]
        files += md_api.get("api/model-files", payload)
    return files


def _truncate_clu_data(md_api: MetadataApi):
    confirmation = input(
        "!!! WARNING !!! This action will permanently delete ALL Cloudnet files from the DVAS production server. !!!\n"
        "Are you ABSOLUTELY sure you want to proceed? (Type 'YES' to confirm, 'no' to cancel): "
    )
    if confirmation.lower() != "yes":
        print("Delete canceled.")
        return
    dvas = Dvas()
    dvas.delete_all()
    files = md_api.get("api/files", {"dvasUpdated": True})
    for file in files:
        md_api.clean_dvas_info(file["uuid"])


def _delete_clu_data(md_api: MetadataApi, files: list[dict]):
    logging.info(f"Deleting {len(files)} CLU files in the DVAS data portal.")
    dvas = Dvas()
    for file in files:
        try:
            dvas.delete(file)
        except DvasError as err:
            logging.error(f"Failed to delete {file['uuid']}: {err}")
        finally:
            md_api.clean_dvas_info(file["uuid"])


def add_arguments(subparser):
    dvas_parser = subparser.add_parser(
        "dvas", help="Manage Cloudnet data " "in the DVAS data portal"
    )

    dvas_parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete all CLU metadata from the DVAS data portal.",
        default=False,
    )
    dvas_parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete selected metadata from the DVAS data portal.",
        default=False,
    )

    return subparser
