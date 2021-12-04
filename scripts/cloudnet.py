#!/usr/bin/env python3
"""A wrapper script for calling data processing functions."""
import sys
import warnings
import argparse

from data_processing import utils
from data_processing.subcmds import freeze, process_cloudnet, process_model, process_model_evaluation, \
                                    create_images, create_qc_reports


warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)

modules = {
    'freeze': freeze,
    'process': process_cloudnet,
    'model': process_model,
    'me': process_model_evaluation,
    'plot': create_images,
    'qc': create_qc_reports
}


def main(args):
    args = _parse_args(args)
    utils.init_logger(args)
    cmd = args.cmd
    modules[cmd].main(args)


def _parse_args(args):
    parser = argparse.ArgumentParser(description='Cloudnet processing main wrapper.',
                                     epilog='Enjoy the program! :)')
    subparsers = parser.add_subparsers(title='Command', help='Command to execute.', required=True, dest='cmd')
    for module in modules.values():
        subparsers = module.add_arguments(subparsers)
    group = parser.add_argument_group(title='General options')
    group.add_argument('-s', '--sites',
                       help='Sites to process data from, e.g. hyytiala,limassol. Default is all Cloudnet sites.',
                       type=lambda s: s.split(','),
                       default=utils.get_cloudnet_sites())
    group.add_argument('-p', '--products',
                       help='Products to be processed, e.g., radar,lidar,mwr,categorize,iwc.\
                        Default is all regular products.',
                       type=lambda s: s.split(','),
                       default=utils.get_product_types())
    group.add_argument('--start',
                       type=str,
                       metavar='YYYY-MM-DD',
                       help='Starting date. Default is current day - 5 (included).',
                       default=utils.get_date_from_past(5))
    group.add_argument('--stop',
                       type=str,
                       metavar='YYYY-MM-DD',
                       help='Stopping date. Default is current day + 1 (excluded).',
                       default=utils.get_date_from_past(-1))
    group.add_argument('-d', '--date',
                       type=str,
                       metavar='YYYY-MM-DD',
                       help='Single date to be processed.')
    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
