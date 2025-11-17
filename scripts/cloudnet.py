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
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TypeVar
from uuid import UUID

from cloudnet_api_client import APIClient
from cloudnet_api_client.containers import (
    ExtendedProduct,
    Instrument,
    ProductMetadata,
    RawMetadata,
    Site,
)
from requests import Session

from processing import utils
from processing.config import Config
from processing.dvas import Dvas
from processing.fetch import fetch
from processing.instrument import process_instrument
from processing.jobs import freeze, update_plots, update_qc, upload_to_dvas
from processing.metadata_api import MetadataApi
from processing.model import process_model
from processing.pid_utils import PidUtils
from processing.processor import (
    ExtendedSite,
    InstrumentParams,
    ModelParams,
    Processor,
    ProductParams,
)
from processing.product import process_product
from processing.storage_api import StorageApi
from processing.utils import SkipTaskError

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

if sys.stdout.isatty() and not os.getenv("NO_COLOR"):
    GREEN = "\033[92m"
    BOLD = "\033[1m"
    RESET = "\033[0m"
else:
    GREEN = ""
    BOLD = ""
    RESET = ""


def main() -> None:
    config = Config()
    session = utils.make_session()
    client = APIClient(f"{config.dataportal_url}/api/", session)
    args = _parse_args(client)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    handler.setFormatter(formatter)
    logger.handlers = [handler]
    process_main(args, config, session, client)


def _parse_args(client: APIClient) -> Namespace:
    parser = argparse.ArgumentParser(
        description="Cloudnet processing main wrapper.",
        epilog="Enjoy the program! :)",
    )

    group = parser.add_argument_group(title="General options")
    group.add_argument(
        "-s",
        "--sites",
        help="Sites to process data from, e.g. hyytiala.",
        type=lambda sites: _validate_sites(sites, client),
        required=True,
    )
    group.add_argument(
        "-p",
        "--products",
        help="Products to be processed, e.g., radar,lidar,mwr.",
        type=lambda products: _validate_products(products, client),
    )
    parser.add_argument(
        "-i",
        "--instruments",
        help="Instrument types to be processed, e.g., mira-35,chm15k,hatpro.",
        type=lambda instruments: _validate_types(instruments, client),
    )
    parser.add_argument(
        "-m",
        "--models",
        help="Models to be processed.",
        type=lambda models: _validate_models(models, client),
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


def process_main(
    args: Namespace, config: Config, session: Session, client: APIClient
) -> None:
    md_api = MetadataApi(config, session)
    storage_api = StorageApi(config, session)
    pid_utils = PidUtils(config, session)
    dvas = Dvas(config, md_api, client)
    processor = Processor(md_api, storage_api, pid_utils, dvas, client)

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
            args.models = {m.id for m in client.models()}

        if not args.products:
            args.products = ["model"]  # just to make loop work (fetch all)

    date = args.start
    while date <= args.stop:
        for site_id in args.sites:
            site = processor.get_site(site_id, date)
            for product_id in args.products:
                product = client.product(product_id)
                try:
                    if args.cmd == "fetch":
                        fetch(product, site, date, args, client)
                    else:
                        _process_file(processor, product, site, date, args)
                except SkipTaskError as err:
                    logging.warning("Skipped task: %s", err)
                except Exception:
                    logging.exception("Failed to process task")
        date += datetime.timedelta(days=1)
    print()


def _update_product_list(args: Namespace, processor: Processor) -> list[str]:
    products = set(args.products) if args.products else set()
    if args.instruments:
        for instrument in args.instruments:
            derived_products = processor.client.instrument_derived_products(instrument)
            if derived_products:
                if args.raw:
                    products.add(list(derived_products)[0])
                else:
                    products.update(derived_products)
    if args.uuids:
        for uuid in args.uuids:
            derived_products = processor.client.instrument(uuid).derived_product_ids
            if derived_products:
                if args.raw:
                    products.add(list(derived_products)[0])
                else:
                    products.update(derived_products)
    return list(products)


def _update_instrument_list(args: Namespace, processor: Processor) -> list[str]:
    return [
        i
        for p in args.products
        for i in processor.client.product(p).source_instrument_ids
    ]


def _process_file(
    processor: Processor,
    product: ExtendedProduct,
    site: Site | ExtendedSite,
    date: datetime.date,
    args: Namespace,
) -> None:
    if product.id == "model":
        if args.cmd in ("dvas", "hkd"):
            raise SkipTaskError(f"{args.cmd.upper()} not supported for model products")
        if args.models:
            model_ids = set(args.models)
        else:
            metadata = processor.client.raw_model_files(
                site_id=site.id, date=date, status=["uploaded", "processed"]
            )
            model_ids = {meta.model.id for meta in metadata}
        for model_id in model_ids:
            model_params = ModelParams(
                site=site,
                date=date,
                product=product,
                model=processor.client.model(model_id),
            )
            _print_header(model_params, args)
            with TemporaryDirectory() as temp_dir:
                directory = Path(temp_dir)
                if args.cmd == "plot":
                    update_plots(processor, model_params, directory)
                elif args.cmd == "qc":
                    update_qc(processor, model_params, directory)
                elif args.cmd == "freeze":
                    freeze(processor, model_params, directory)
                else:
                    process_model(processor, model_params, directory)
    elif product.source_instrument_ids:
        # Instrument products
        if args.cmd == "dvas":
            raise SkipTaskError("DVAS not supported for instrument products")
        if args.uuids:
            instruments = _get_instruments_for_product(processor.client, product, args)
        else:
            valid_instruments = list(
                (set(args.instruments) & product.source_instrument_ids)
                if args.instruments
                else product.source_instrument_ids
            )
            file_meta: list[ProductMetadata] | list[RawMetadata]
            file_meta = processor.client.raw_files(
                site_id=site.id,
                date=date,
                instrument_id=valid_instruments,
            )
            if not file_meta:
                # No raw data, but we can still have fetched products
                file_meta = processor.client.files(
                    site_id=site.id,
                    date=date,
                    instrument_id=valid_instruments,
                )
            instruments = {m.instrument for m in file_meta if m.instrument}
        for instrument in instruments:
            instru_params = InstrumentParams(
                site=site, date=date, product=product, instrument=instrument
            )
            _print_header(instru_params, args)
            with TemporaryDirectory() as temp_dir:
                directory = Path(temp_dir)
                if args.cmd == "plot":
                    update_plots(processor, instru_params, directory)
                elif args.cmd == "qc":
                    update_qc(processor, instru_params, directory)
                elif args.cmd == "freeze":
                    freeze(processor, instru_params, directory)
                elif args.cmd == "hkd":
                    processor.process_housekeeping(instru_params)
                else:
                    try:
                        process_instrument(processor, instru_params, directory)
                    except SkipTaskError as err:
                        logging.warning("Skipped task: %s", err)
    elif product.id in ("mwr-single", "mwr-multi", "epsilon-lidar"):
        if args.cmd in ("dvas", "hkd"):
            raise SkipTaskError(f"{args.cmd.upper()} not supported for {product.id}")
        if args.uuids:
            instruments = _get_instruments_for_product(processor.client, product, args)
        else:
            mapping = {
                "mwr-single": "mwr-l1c",
                "mwr-multi": "mwr-l1c",
                "epsilon-lidar": "doppler-lidar",
            }
            product_metadata = processor.client.files(
                site_id=site.id,
                date=date,
                product_id=mapping.get(product.id),
            )
            instruments = {m.instrument for m in product_metadata if m.instrument}
        for instrument in instruments:
            product_params = ProductParams(
                site=site,
                date=date,
                product=product,
                instrument=instrument,
            )
            _print_header(product_params, args)
            with TemporaryDirectory() as temp_dir:
                directory = Path(temp_dir)
                if args.cmd == "plot":
                    update_plots(processor, product_params, directory)
                elif args.cmd == "qc":
                    update_qc(processor, product_params, directory)
                elif args.cmd == "freeze":
                    freeze(processor, product_params, directory)
                else:
                    process_product(processor, product_params, directory)
    elif product.id in ("l3-cf", "l3-iwc", "l3-lwc"):
        params = ModelParams(
            site=site,
            date=date,
            product=product,
            model=processor.client.model("ecmwf"),  # Hard coded for now.
        )
        _print_header(params, args)
        with TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            if args.cmd == "plot":
                update_plots(processor, params, directory)
            elif args.cmd == "qc":
                update_qc(processor, params, directory)
            elif args.cmd == "freeze":
                freeze(processor, params, directory)
            elif args.cmd in ("dvas", "hkd"):
                raise SkipTaskError(f"{args.cmd.upper()} not supported for L3 products")
            else:
                process_product(processor, params, directory)
    else:
        product_params = ProductParams(
            site=site,
            date=date,
            product=product,
            instrument=None,
        )
        _print_header(product_params, args)
        with TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            if args.cmd == "plot":
                update_plots(processor, product_params, directory)
            elif args.cmd == "qc":
                update_qc(processor, product_params, directory)
            elif args.cmd == "freeze":
                freeze(processor, product_params, directory)
            elif args.cmd == "dvas":
                upload_to_dvas(processor, product_params)
            elif args.cmd == "hkd":
                raise SkipTaskError("HKD not supported for geophysical products")
            else:
                process_product(processor, product_params, directory)


def _get_instruments_for_product(
    client: APIClient, product: ExtendedProduct, args: Namespace
) -> set[Instrument]:
    instruments = {client.instrument(uuid) for uuid in args.uuids}
    return {i for i in instruments if i.instrument_id in product.source_instrument_ids}


def _validate_types(types: str, client: APIClient) -> list[str]:
    input_types = types.split(",")
    valid_types = client.instrument_ids()
    if invalid_types := set(input_types) - valid_types:
        raise ArgumentTypeError("Invalid instrument types: " + ", ".join(invalid_types))
    return input_types


def _validate_sites(sites: str, client: APIClient) -> list[str]:
    if sites == "cloudnet":
        return [s.id for s in client.sites() if "cloudnet" in s.type]
    input_sites = sites.split(",")
    valid_sites = {s.id for s in client.sites()}
    if invalid_sites := set(input_sites) - valid_sites:
        raise ArgumentTypeError("Invalid sites: " + ", ".join(invalid_sites))
    return input_sites


def _validate_models(models: str, client: APIClient) -> list[str]:
    input_models = models.split(",")
    valid_models = {m.id for m in client.models()}
    if invalid_models := set(input_models) - valid_models:
        raise ArgumentTypeError("Invalid models: " + ", ".join(invalid_models))
    return input_models


def _validate_products(products: str, client: APIClient) -> list[str]:
    product_list = products.split(",")
    valid_products = {p.id for p in client.products()}
    accepted_products = []
    rejected_products = []
    for prod in product_list:
        match prod:
            case "instrument" | "geophysical" | "evaluation":
                product_types = [p.id for p in client.products(type=prod)]
                accepted_products.extend(product_types)
            case "voodoo":
                product_types = ["categorize-voodoo", "classification-voodoo"]
                accepted_products.extend(product_types)
            case "mwrpy":
                product_types = ["mwr-l1c", "mwr-single", "mwr-multi"]
                accepted_products.extend(product_types)
            case "doppy":
                product_types = ["doppler-lidar", "doppler-lidar-wind", "epsilon-lidar"]
                accepted_products.extend(product_types)
            case "cpr":
                accepted_products.extend(
                    ["cpr-simulation", "cpr-validation", "cpr-tc-validation"]
                )
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


def _print_header(
    params: ModelParams | InstrumentParams | ProductParams, args: Namespace
) -> None:
    the_dict = asdict(params)
    parts = [f"{BOLD}Task:{RESET} {GREEN}{args.cmd}{RESET}"]
    parts.extend(
        [
            f"{BOLD}{key.capitalize()}:{RESET} {GREEN}{value.get('id') or value.get('instrument_id') or value if isinstance(value, dict) else value}{RESET}"
            for key, value in the_dict.items()
            if value is not None
        ]
    )
    if hasattr(params, "instrument") and params.instrument:
        parts.append(
            f"{BOLD}Instrument PID:{RESET} {GREEN}{params.instrument.pid}{RESET}"
        )
    print()
    print("  ".join(parts))


def _print_fetch_header(args: Namespace) -> None:
    print()
    msg = "Fetching raw data" if args.raw else "Fetching products"
    print(f"{BOLD}{msg}:{RESET}")
    if not args.raw:
        print()


if __name__ == "__main__":
    main()
