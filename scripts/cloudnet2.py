#!/usr/bin/env python3
"""A wrapper script for calling data processing functions."""

import argparse
import datetime
import logging
import os
import re
import sys
import warnings
from argparse import ArgumentTypeError, Namespace
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TypeVar
from uuid import UUID

from data_processing import utils
from data_processing.dvas import Dvas
from data_processing.metadata_api import MetadataApi
from data_processing.pid_utils import PidUtils
from data_processing.storage_api import StorageApi
from processing.instrument import process_instrument
from processing.model import process_model
from processing.processor import (
    Instrument,
    InstrumentParams,
    ModelParams,
    Processor,
    Product,
    ProductParams,
    Site,
)
from processing.product import process_product
from processing.utils import utctoday

pattern = re.compile("overflow encountered in (multiply|divide)")
warnings.filterwarnings(
    "ignore",
    category=RuntimeWarning,
    message=pattern.pattern,
    module="matplotlib.colors",
)

# Investigate these warnings later:
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message="Warning: 'partition' will ignore the 'mask' of the MaskedArray.",
    module="numpy",
)

if sys.stdout.isatty() and not os.getenv("NO_COLOR"):
    GREEN = "\033[92m"
    BOLD = "\033[1m"
    RESET = "\033[0m"
else:
    GREEN = ""
    BOLD = ""
    RESET = ""


def main():
    args = _parse_args()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    handler.setFormatter(formatter)
    logger.handlers = [handler]
    process_main(args)


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Cloudnet processing main wrapper.",
        epilog="Enjoy the program! :)",
    )
    group = parser.add_argument_group(title="General options")
    group.add_argument(
        "-s",
        "--sites",
        help="Sites to process data from, e.g. hyytiala.",
        type=validate_sites,
    )
    group.add_argument(
        "-p",
        "--products",
        help="Products to be processed, e.g., radar,lidar,mwr.",
        type=validate_products,
        required=True,
    )
    parser.add_argument(
        "-t",
        "--types",
        help="Instrument types to be processed, e.g., mira,chm15k,hatpro.",
        type=list_parser(str),
    )
    parser.add_argument(
        "-m",
        "--models",
        help="Models to be processed.",
        type=validate_models,
    )
    parser.add_argument(
        "-i",
        "--instruments",
        help="Instrument UUIDs to be processed.",
        type=list_parser(UUID),
    )
    group.add_argument(
        "--start",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="Starting date. Default is five days ago.",
    )
    group.add_argument(
        "--stop",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="Stopping date. Default is current day.",
    )
    group.add_argument(
        "-d",
        "--date",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="Date to be processed.",
    )

    args = parser.parse_args()

    if args.date and (args.start or args.stop):
        print("Cannot use --date with --start and --stop", file=sys.stderr)
        sys.exit(1)
    if args.date:
        args.start = args.date
        args.stop = args.date
    else:
        if not args.start:
            args.start = utctoday() - datetime.timedelta(days=5)
        if not args.stop:
            args.stop = utctoday()
        if args.start > args.stop:
            print("--start should be before --stop", file=sys.stderr)
            sys.exit(1)
    del args.date

    return args


def parse_instrument(meta: dict) -> Instrument:
    return Instrument(
        uuid=meta["instrumentInfo"]["uuid"],
        pid=meta["instrumentInfo"]["pid"],
        type=meta["instrument"]["id"],
    )


def print_header(data):
    parts = [
        f"{BOLD}{key}:{RESET} {GREEN}{value}{RESET}" for key, value in data.items()
    ]
    print("  ".join(parts))


def process_main(args):
    config = utils.read_main_conf()
    session = utils.make_session()
    md_api = MetadataApi(config, session)
    storage_api = StorageApi(config, session)
    pid_utils = PidUtils(config, session)
    dvas = Dvas(config, md_api)
    processor = Processor(md_api, storage_api, pid_utils, dvas)

    date = args.start
    while date <= args.stop:
        for site_id in args.sites:
            for product_id in args.products:
                site = processor.get_site(site_id)
                product = processor.get_product(product_id)
                print()
                try:
                    process_file(processor, product, site, date, args)
                except utils.SkipTaskError as err:
                    logging.warning("Skipped task: %s", err)
                except Exception:
                    logging.exception("Failed to process task")
        date += datetime.timedelta(days=1)


def process_file(
    processor: Processor,
    product: Product,
    site: Site,
    date: datetime.date,
    args: Namespace,
):
    if product.id == "model":
        if args.models:
            model_ids = set(args.models)
        else:
            payload = {
                "site": site.id,
                "date": date.isoformat(),
                "allModels": True,
            }
            metadata = processor.md_api.get("api/raw-model-files", payload)
            model_ids = set(meta["model"]["id"] for meta in metadata)
        for model_id in model_ids:
            print_header(
                {
                    "Site": site.id,
                    "Date": date.isoformat(),
                    "Product": product.id,
                    "Model": model_id,
                }
            )
            model_params = ModelParams(
                site=site,
                date=date,
                product=product,
                model_id=model_id,
            )
            with TemporaryDirectory() as directory:
                process_model(processor, model_params, Path(directory))
    elif product.source_instrument_ids:
        if args.instruments:
            instruments = {processor.get_instrument(uuid) for uuid in args.instruments}
        else:
            payload = {
                "site": site.id,
                "date": date.isoformat(),
                "instrument": product.source_instrument_ids,
            }
            metadata = processor.md_api.get("api/raw-files", payload)
            instruments = {parse_instrument(meta) for meta in metadata}
        for instrument in instruments:
            print_header(
                {
                    "Site": site.id,
                    "Date": date.isoformat(),
                    "Product": product.id,
                    "Instrument": instrument.type,
                    "Instrument PID": instrument.pid,
                }
            )
            instru_params = InstrumentParams(
                site=site, date=date, product=product, instrument=instrument
            )
            with TemporaryDirectory() as directory:
                process_instrument(processor, instru_params, Path(directory))
    elif product.id in ("mwr-single", "mwr-multi"):
        if args.instruments:
            instruments = {processor.get_instrument(uuid) for uuid in args.instruments}
        else:
            payload = {
                "site": site.id,
                "date": date.isoformat(),
                "product": "mwr-l1c",
            }
            metadata = processor.md_api.get("api/files", payload)
            instrument_uuids = {meta["instrumentInfoUuid"] for meta in metadata}
            instruments = {processor.get_instrument(uuid) for uuid in instrument_uuids}
        for instrument in instruments:
            print_header(
                {
                    "Site": site.id,
                    "Date": date.isoformat(),
                    "Product": product.id,
                    "Instrument": instrument.type,
                    "Instrument PID": instrument.pid,
                }
            )
            product_params = ProductParams(
                site=site,
                date=date,
                product=product,
                instrument=instrument,
            )
            with TemporaryDirectory() as directory:
                process_product(processor, product_params, Path(directory))
    else:
        print_header({"Site": site.id, "Date": date.isoformat(), "Product": product.id})
        product_params = ProductParams(
            site=site,
            date=date,
            product=product,
            instrument=None,
        )
        with TemporaryDirectory() as directory:
            process_product(processor, product_params, Path(directory))


def validate_sites(sites: str) -> list[str]:
    input_sites = sites.split(",")
    valid_sites = set(utils.get_all_but_hidden_sites())
    if invalid_sites := set(input_sites) - valid_sites:
        raise ArgumentTypeError("Invalid sites: " + ", ".join(invalid_sites))
    return input_sites


def validate_models(models: str) -> list[str]:
    input_models = models.split(",")
    valid_models = set(utils.get_model_types())
    if invalid_models := set(input_models) - valid_models:
        raise ArgumentTypeError("Invalid models: " + ", ".join(invalid_models))
    return input_models


def validate_products(products: str) -> list[str]:
    product_list = products.split(",")
    valid_products = utils.get_product_types()
    accepted_products = []
    rejected_products = []
    for prod in product_list:
        match prod:
            case "l1b" | "l1c" | "l2":
                product_types = utils.get_product_types(prod[1:])
                accepted_products.extend(product_types)
            case "voodoo":
                product_types = ["categorize-voodoo", "classification-voodoo"]
                accepted_products.extend(product_types)
            case "mwrpy":
                product_types = ["mwr-l1c", "mwr-single", "mwr-multi"]
                accepted_products.extend(product_types)
            case "standard":
                product_types = utils.get_product_types_excluding_level3(
                    ignore_experimental=True
                )
                accepted_products.extend(product_types)
            case "doppy":
                product_types = ["doppler-lidar", "doppler-lidar-wind"]
                accepted_products.extend(product_types)
            case prod if prod in valid_products:
                accepted_products.append(prod)
            case prod:
                rejected_products.append(prod)
    if rejected_products:
        raise ArgumentTypeError("Invalid products: " + ", ".join(rejected_products))
    return accepted_products


T = TypeVar("T")


def list_parser(type: Callable[[str], T]) -> Callable[[str], list[T]]:
    return lambda value: [type(x) for x in value.split(",")]


def parse_date(value: str) -> datetime.date:
    if value == "today":
        return utctoday()
    if value == "yesterday":
        return utctoday() - datetime.timedelta(days=1)
    if match := re.fullmatch("(\d+)d", value):
        return utctoday() - datetime.timedelta(days=int(match[1]))
    return datetime.date.fromisoformat(value)


if __name__ == "__main__":
    main()
