#!/usr/bin/env python3
"""A wrapper script for calling data processing functions."""

import argparse
import calendar
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

from processing import utils
from processing.dvas import Dvas
from processing.fetch import fetch
from processing.instrument import process_instrument
from processing.jobs import freeze, hkd, update_plots, update_qc, upload_to_dvas
from processing.metadata_api import MetadataApi
from processing.model import process_model
from processing.pid_utils import PidUtils
from processing.processor import (
    InstrumentParams,
    ModelParams,
    Processor,
    Product,
    ProductParams,
    Site,
)
from processing.product import process_me, process_product
from processing.storage_api import StorageApi

logging.basicConfig(level=logging.INFO)

warnings.filterwarnings(
    "ignore",
    category=RuntimeWarning,
    message="overflow encountered in (multiply|divide)",
    module="matplotlib.colors",
)

# TODO: Investigate these from model-evaluation:
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message="The input coordinates to pcolormesh are interpreted as cell centers",
)

# TODO: Investigate these warnings later:
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
        type=_validate_sites,
        required=True,
    )
    group.add_argument(
        "-p",
        "--products",
        help="Products to be processed, e.g., radar,lidar,mwr.",
        type=_validate_products,
    )
    parser.add_argument(
        "-i",
        "--instruments",
        help="Instrument types to be processed, e.g., mira-35,chm15k,hatpro.",
        type=_validate_types,
    )
    parser.add_argument(
        "-m",
        "--models",
        help="Models to be processed.",
        type=_validate_models,
    )
    parser.add_argument(
        "-u",
        "--uuids",
        help="Instrument UUIDs to be processed.",
        type=_list_parser(UUID),
    )
    group.add_argument(
        "--start",
        type=lambda value: _parse_date(value)[0],
        metavar="YYYY-MM-DD",
        help="Starting date. Default is five days ago.",
    )
    group.add_argument(
        "--stop",
        type=lambda value: _parse_date(value)[1],
        metavar="YYYY-MM-DD",
        help="Stopping date. Default is current day.",
    )
    group.add_argument(
        "-d",
        "--date",
        type=_parse_date,
        metavar="YYYY-MM-DD",
        help="Date to be processed.",
    )
    group.add_argument(
        "-c",
        "--cmd",
        required=False,
        default="process",
        choices=["process", "plot", "qc", "freeze", "dvas", "fetch", "hkd"],
        help="Command.",
    )
    group.add_argument(
        "--raw",
        action="store_true",
        help="Fetch raw data. Only applicable if the command is 'fetch'.",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Fetch ALL raw data including .LV0. Only applicable if the command is 'fetch --raw'.",
    )

    args = parser.parse_args()

    if not (args.cmd == "fetch" and args.raw) and not (
        args.products or args.instruments or args.models or args.uuids
    ):
        print(
            "Please provide --products, --instruments, --models, --uuids or --raw to continue.",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.date and (args.start or args.stop):
        parser.error("Cannot use --date with --start and --stop")
    if args.date:
        args.start, args.stop = args.date
    elif args.start and args.stop and args.start > args.stop:
        parser.error("--start should be before --stop")
    if not args.start:
        args.start = utils.utctoday()
    if not args.stop:
        args.stop = utils.utctoday()
    del args.date

    return args


def process_main(args):
    config = utils.read_main_conf()
    session = utils.make_session()
    md_api = MetadataApi(config, session)
    storage_api = StorageApi(config, session)
    pid_utils = PidUtils(config, session)
    dvas = Dvas(config, md_api)
    processor = Processor(md_api, storage_api, pid_utils, dvas)

    if args.cmd == "fetch":
        _print_fetch_header(args)

    if not args.products or args.products == ["model"]:
        args.products = _update_product_list(args, processor)

    if args.models and "model" not in args.products:
        args.products.append("model")

    if args.cmd == "fetch" and args.raw:
        if (args.products or args.uuids) and not args.instruments:
            args.instruments = _update_instrument_list(args, processor)

        if "model" in args.products and not args.models:
            args.models = _get_model_types()

        if not args.products:
            args.products = ["model"]  # just to make loop work (fetch all)

    date = args.start
    while date <= args.stop:
        for site_id in args.sites:
            site = processor.get_site(site_id, date)
            for product_id in args.products:
                product = processor.get_product(product_id)
                try:
                    if args.cmd == "fetch":
                        fetch(product, site, date, args)
                    else:
                        _process_file(processor, product, site, date, args)
                except utils.SkipTaskError as err:
                    logging.warning("Skipped task: %s", err)
                except Exception:
                    logging.exception("Failed to process task")
        date += datetime.timedelta(days=1)
    print()


def _update_product_list(args: Namespace, processor: Processor) -> list[str]:
    products = set(args.products) if args.products else set()
    if args.instruments:
        for instrument in args.instruments:
            derived_products = list(processor.get_derived_products(instrument))
            if len(derived_products) > 0:
                if args.raw:
                    products.update([derived_products[0]])
                else:
                    products.update(derived_products)
    if args.uuids:
        for uuid in args.uuids:
            derived_products = list(processor.get_instrument(uuid).derived_product_ids)
            if len(derived_products) > 0:
                if args.raw:
                    products.update([derived_products[0]])
                else:
                    products.update(derived_products)
    return list(products)


def _update_instrument_list(args: Namespace, processor: Processor) -> list[str]:
    return [
        i for p in args.products for i in processor.get_product(p).source_instrument_ids
    ]


def _process_file(
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
            _print_header(
                {
                    "Task": args.cmd,
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
                model=processor.get_model(model_id),
            )
            with TemporaryDirectory() as directory:
                if args.cmd == "plot":
                    update_plots(processor, model_params, Path(directory))
                elif args.cmd == "qc":
                    update_qc(processor, model_params, Path(directory))
                elif args.cmd == "freeze":
                    freeze(processor, model_params, Path(directory))
                elif args.cmd == "dvas":
                    raise utils.SkipTaskError("DVAS not supported for model products")
                else:
                    process_model(processor, model_params, Path(directory))
    elif product.source_instrument_ids:
        if args.uuids:
            instruments = {processor.get_instrument(uuid) for uuid in args.uuids}
        else:
            payload = {
                "site": site.id,
                "date": date.isoformat(),
                "instrument": args.instruments or product.source_instrument_ids,
            }
            metadata = processor.md_api.get("api/raw-files", payload)
            # Need to get instrument again because derivedProductIds is missing from raw-files response...
            if metadata:
                instruments = {
                    processor.get_instrument(meta["instrumentInfo"]["uuid"])
                    for meta in metadata
                }
            else:
                # No raw data, but we can still have fetched products
                metadata = processor.md_api.get("api/files", payload)
                instruments = {
                    processor.get_instrument(meta["instrument"]["uuid"])
                    for meta in metadata
                }

        for instrument in instruments:
            _print_header(
                {
                    "Task": args.cmd,
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
                if args.cmd == "plot":
                    update_plots(processor, instru_params, Path(directory))
                elif args.cmd == "qc":
                    update_qc(processor, instru_params, Path(directory))
                elif args.cmd == "freeze":
                    freeze(processor, instru_params, Path(directory))
                elif args.cmd == "hkd":
                    hkd(processor, instru_params)
                elif args.cmd == "dvas":
                    raise utils.SkipTaskError(
                        "DVAS not supported for instrument products"
                    )
                else:
                    try:
                        process_instrument(processor, instru_params, Path(directory))
                    except utils.SkipTaskError as err:
                        logging.warning("Skipped task: %s", err)
    elif product.id in ("mwr-single", "mwr-multi", "epsilon-lidar"):
        if args.uuids:
            instruments = {processor.get_instrument(uuid) for uuid in args.uuids}
        elif product.id in ("mwr-single", "mwr-multi"):
            payload = {
                "site": site.id,
                "date": date.isoformat(),
                "product": "mwr-l1c",
            }
            metadata = processor.md_api.get("api/files", payload)
            instrument_uuids = {meta["instrument"]["uuid"] for meta in metadata}
            instruments = {processor.get_instrument(uuid) for uuid in instrument_uuids}
        else:
            payload = {
                "site": site.id,
                "date": date.isoformat(),
                "product": "doppler-lidar",
            }
            metadata = processor.md_api.get("api/files", payload)
            instrument_uuids = {meta["instrument"]["uuid"] for meta in metadata}
            instruments = {processor.get_instrument(uuid) for uuid in instrument_uuids}
        for instrument in instruments:
            _print_header(
                {
                    "Task": args.cmd,
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
                if args.cmd == "plot":
                    update_plots(processor, product_params, Path(directory))
                elif args.cmd == "qc":
                    update_qc(processor, product_params, Path(directory))
                elif args.cmd == "freeze":
                    freeze(processor, product_params, Path(directory))
                elif args.cmd == "dvas":
                    raise utils.SkipTaskError(
                        "DVAS not supported for instrument products"
                    )
                else:
                    process_product(processor, product_params, Path(directory))
    elif product.id in ("l3-cf", "l3-iwc", "l3-lwc"):
        model_id = "ecmwf"  # Hard coded for now. Add support for other models later.
        _print_header(
            {
                "Task": args.cmd,
                "Site": site.id,
                "Date": date.isoformat(),
                "Product": product.id,
                "Model": model_id,
            }
        )
        params = ModelParams(
            site=site,
            date=date,
            product=product,
            model=processor.get_model(model_id),
        )
        with TemporaryDirectory() as directory:
            if args.cmd == "plot":
                update_plots(processor, params, Path(directory))
            elif args.cmd == "qc":
                update_qc(processor, params, Path(directory))
            elif args.cmd == "freeze":
                freeze(processor, params, Path(directory))
            elif args.cmd == "dvas":
                raise utils.SkipTaskError("DVAS not supported for L3 products")
            else:
                process_me(processor, params, Path(directory))
    else:
        _print_header(
            {
                "Task": args.cmd,
                "Site": site.id,
                "Date": date.isoformat(),
                "Product": product.id,
            }
        )
        product_params = ProductParams(
            site=site,
            date=date,
            product=product,
            instrument=None,
        )
        with TemporaryDirectory() as directory:
            if args.cmd == "plot":
                update_plots(processor, product_params, Path(directory))
            elif args.cmd == "qc":
                update_qc(processor, product_params, Path(directory))
            elif args.cmd == "freeze":
                freeze(processor, product_params, Path(directory))
            elif args.cmd == "dvas":
                upload_to_dvas(processor, product_params)
            else:
                process_product(processor, product_params, Path(directory))


def _validate_types(types: str) -> list[str]:
    input_types = types.split(",")
    valid_types = set(_get_instrument_types())
    if invalid_types := set(input_types) - valid_types:
        raise ArgumentTypeError("Invalid instrument types: " + ", ".join(invalid_types))
    return input_types


def _validate_sites(sites: str) -> list[str]:
    input_sites = sites.split(",")
    valid_sites = set(_get_all_but_hidden_sites())
    if invalid_sites := set(input_sites) - valid_sites:
        raise ArgumentTypeError("Invalid sites: " + ", ".join(invalid_sites))
    return input_sites


def _validate_models(models: str) -> list[str]:
    input_models = models.split(",")
    valid_models = set(_get_model_types())
    if invalid_models := set(input_models) - valid_models:
        raise ArgumentTypeError("Invalid models: " + ", ".join(invalid_models))
    return input_models


def _validate_products(products: str) -> list[str]:
    product_list = products.split(",")
    valid_products = utils.get_product_types()
    accepted_products = []
    rejected_products = []
    for prod in product_list:
        match prod:
            case "instrument" | "geophysical" | "evaluation":
                product_types = utils.get_product_types(prod)
                accepted_products.extend(product_types)
            case "voodoo":
                product_types = ["categorize-voodoo", "classification-voodoo"]
                accepted_products.extend(product_types)
            case "mwrpy":
                product_types = ["mwr-l1c", "mwr-single", "mwr-multi"]
                accepted_products.extend(product_types)
            case "doppy":
                product_types = ["doppler-lidar", "doppler-lidar-wind"]
                accepted_products.extend(product_types)
            case "cpr":
                accepted_products.extend(["cpr-simulation"])
            case prod if prod in valid_products:
                accepted_products.append(prod)
            case prod:
                rejected_products.append(prod)
    if rejected_products:
        raise ArgumentTypeError("Invalid products: " + ", ".join(rejected_products))
    return accepted_products


T = TypeVar("T")


def _list_parser(type: Callable[[str], T]) -> Callable[[str], list[T]]:
    return lambda value: [type(x) for x in value.split(",")]


def _parse_date(value: str) -> tuple[datetime.date, datetime.date]:
    if value == "today":
        date = utils.utctoday()
        return (date, date)
    if value == "yesterday":
        date = utils.utctoday() - datetime.timedelta(days=1)
        return (date, date)
    if match := re.fullmatch(r"(\d+)d", value):
        date = utils.utctoday() - datetime.timedelta(days=int(match[1]))
        return (date, date)
    match list(map(int, value.split("-"))):
        case [year, month, day]:
            date = datetime.date(year, month, day)
            return (date, date)
        case [year, month]:
            last_day = calendar.monthrange(year, month)[1]
            return (datetime.date(year, month, 1), datetime.date(year, month, last_day))
        case [year]:
            return (datetime.date(year, 1, 1), datetime.date(year, 12, 31))
        case invalid:
            raise ValueError(f"Invalid date: {invalid}")


def _print_header(data):
    parts = [
        f"{BOLD}{key}:{RESET} {GREEN}{value}{RESET}" for key, value in data.items()
    ]
    print()
    print("  ".join(parts))


def _print_fetch_header(args: Namespace):
    print()
    msg = "Fetching raw data" if args.raw else "Fetching products"
    print(f"{BOLD}{msg}:{RESET}")
    if not args.raw:
        print()


def _get_all_but_hidden_sites() -> list:
    """Returns all but hidden site identifiers."""
    sites = utils.get_from_data_portal_api("api/sites")
    return [site["id"] for site in sites if "hidden" not in site["type"]]


def _get_model_types() -> list:
    """Returns list of model types."""
    models = utils.get_from_data_portal_api("api/models")
    return [model["id"] for model in models]


def _get_instrument_types() -> list:
    instruments = utils.get_from_data_portal_api("api/instruments")
    return list(set([i["id"] for i in instruments]))


if __name__ == "__main__":
    main()
