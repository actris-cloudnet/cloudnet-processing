#!/usr/bin/env python3
"""A wrapper script for calling data processing functions."""
import argparse
import sys
import warnings
from collections import OrderedDict
from tempfile import NamedTemporaryFile

from data_processing import utils
from data_processing.subcmds import (
    create_images,
    create_qc_reports,
    freeze,
    process_cloudnet,
    process_housekeeping,
    process_model,
    process_model_evaluation,
)

warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)

"""All modules MUST have an add_arguments function which adds the subcommand to the subparser."""
modules = {
    "freeze": freeze,
    "process": process_cloudnet,
    "model": process_model,
    "me": process_model_evaluation,
    "plot": create_images,
    "qc": create_qc_reports,
    "housekeeping": process_housekeeping,
}


def main(args):
    args = _parse_args(args)
    logfile = NamedTemporaryFile()
    utils.init_logger(args, logfile.name)
    args.log_filename = logfile.name
    cmd = args.cmd
    modules[cmd].main(args)


def _parse_args(args):
    parser = argparse.ArgumentParser(
        description="Cloudnet processing main wrapper.", epilog="Enjoy the program! :)"
    )
    subparsers = parser.add_subparsers(
        title="Command", help="Command to execute.", required=True, dest="cmd"
    )
    for module in modules.values():
        subparsers = module.add_arguments(subparsers)
    group = parser.add_argument_group(title="General options")
    group.add_argument(
        "-s",
        "--site",
        required=True,
        help="Site to process data from, e.g. hyytiala",
        type=str,
    )
    group.add_argument(
        "-p",
        "--products",
        help="Products to be processed, e.g., radar,lidar,mwr,categorize,iwc.\
                        Default is all regular products.",
        type=lambda s: s.split(","),
        default=utils.get_product_types_excluding_level3(),
    )
    group.add_argument(
        "--start",
        type=str,
        metavar="YYYY-MM-DD",
        help="Starting date. Default is current day - 5 (included).",
        default=utils.get_date_from_past(5),
    )
    group.add_argument(
        "--stop",
        type=str,
        metavar="YYYY-MM-DD",
        help="Stopping date. Default is current day + 1 (excluded).",
        default=utils.get_date_from_past(-1),
    )
    group.add_argument(
        "-d",
        "--date",
        type=str,
        metavar="YYYY-MM-DD",
        help="Single date to be processed.",
    )

    args_parsed = parser.parse_args(args)
    valid_products = validate_products(args_parsed.products)
    if len(valid_products) == 0:
        raise ValueError("No valid products were given.")
    args_parsed.products = valid_products
    return args_parsed


def validate_products(products: list) -> list:
    """Returns a list of products to be processed."""
    valid_products = utils.get_product_types_excluding_level3()
    accepted_products = []
    for prod in products:
        if prod in valid_products:
            accepted_products.append(prod)
        if prod in ("l1b", "l1c", "l2"):
            accepted_products.extend(utils.get_product_types(prod[1:]))
    return list(OrderedDict.fromkeys(accepted_products))  # unique values


if __name__ == "__main__":
    main(sys.argv[1:])
