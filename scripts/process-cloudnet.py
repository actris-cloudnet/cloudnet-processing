#!../prod_venv/bin/python3
"""Master script for CloudnetPy backward processing."""
import os
import argparse
import importlib
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.instruments import rpg2nc, ceilo2nc
from cloudnetpy import utils
process_utils = importlib.import_module("operational-processing").utils
file_paths = importlib.import_module("operational-processing").file_paths


def main():

    if ARGS.keep_uuid:
        raise NotImplementedError

    config = process_utils.read_conf(ARGS)
    site_name = ARGS.site[0]
    site_info = process_utils.read_site_info(site_name)

    start_date = process_utils.date_string_to_date(ARGS.start)
    stop_date = process_utils.date_string_to_date(ARGS.stop)

    for date in utils.date_range(start_date, stop_date):
        dvec = date.strftime("%Y%m%d")
        print('Date: ', dvec)
        obj = file_paths.FilePaths(dvec, config, site_info)
        try:
            for processing_type in ('radar', 'lidar', 'categorize'):
                _process_level1(processing_type, obj)
            for product in ('classification', 'iwc-Z-T-method',
                            'lwc-scaled-adiabatic', 'drizzle'):
                _process_level2(product, obj)
        except (UncalibratedFileMissing, CalibratedFileMissing, RuntimeError) as error:
            print(error)
        print(' ')


def _process_level1(process_type, obj):
    module = importlib.import_module(__name__)
    getattr(module, f"_process_{process_type}")(obj)


def _process_radar(obj):
    output_file = obj.build_calibrated_file_name('radar')
    if obj.config['site']['INSTRUMENTS']['radar'] == 'rpg-fmcw-94':
        rpg_path = obj.build_rpg_path()
        _ = _find_input_file(rpg_path, '*.LV1')
        if _is_writable(output_file):
            print(f"Calibrating rpg-fmcw-94 cloud radar..")
            rpg2nc(rpg_path, output_file, obj.site_info)


def _process_lidar(obj):
    input_path = obj.build_standard_path('uncalibrated', 'lidar')
    input_file = _find_input_file(input_path, f"*{obj.dvec[3:]}*")
    output_file = obj.build_calibrated_file_name('lidar')
    if _is_writable(output_file):
        print(f"Calibrating {obj.config['site']['INSTRUMENTS']['lidar']} lidar..")
        try:
            ceilo2nc(input_file, output_file, obj.site_info)
        except RuntimeError as error:
            raise error


def _find_input_file(path, pattern):
    try:
        return process_utils.find_file(path, pattern)
    except FileNotFoundError:
        raise UncalibratedFileMissing("Can't find uncalibrated input file.")


def _process_categorize(obj):
    input_files = {key: obj.build_calibrated_file_name(key, makedir=False)
                   for key in ['radar', 'lidar', 'model']}
    input_files['mwr'] = obj.build_mwr_file_name()
    for file in input_files.values():
        if not os.path.isfile(file):
            raise CalibratedFileMissing
    output_file = obj.build_standard_output_file_name()
    if _is_writable(output_file):
        try:
            print(f"Processing categorize file..")
            generate_categorize(input_files, output_file)
        except RuntimeError as error:
            raise error


def _process_level2(product, obj):
    categorize_file = obj.build_standard_output_file_name()
    if not os.path.isfile(categorize_file):
        raise CategorizeFileMissing
    output_file = obj.build_standard_output_file_name(product=product)
    product_prefix = product.split('-')[0]
    module = importlib.import_module(f"cloudnetpy.products.{product_prefix}")
    if _is_writable(output_file):
        try:
            print(f"Processing {product} product..")
            getattr(module, f"generate_{product_prefix}")(categorize_file, output_file)
        except ValueError:
            raise RuntimeError(f"Something went wrong with {product} processing.")


def _is_writable(output_file):
    if ARGS.overwrite or not os.path.isfile(output_file):
        return True
    return False


class UncalibratedFileMissing(Exception):
    pass


class CalibratedFileMissing(Exception):
    pass


class CategorizeFileMissing(Exception):
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Cloudnet data.')
    parser.add_argument('site', nargs='+', help='Site Name', choices=['bucharest', 'norunda'])
    parser.add_argument('--start', type=str, metavar='YYYY-MM-DD', help='Starting date. Default is current day - 7.',
                        default=process_utils.get_date_from_past(7))
    parser.add_argument('--stop', type=str, metavar='YYYY-MM-DD', help='Stopping date. Default is current day - 1.',
                        default=process_utils.get_date_from_past(1))
    parser.add_argument('--input', type=str, metavar='/FOO/BAR', help='Input folder path. '
                                                                      'Overrides config/main.ini value.')
    parser.add_argument('--output', type=str, metavar='/FOO/BAR', help='Output folder path. '
                                                                       'Overrides config/main.ini value.')
    parser.add_argument('-o', '--overwrite', dest='overwrite', action='store_true',
                        help='Overwrites data in existing files', default=False)
    parser.add_argument('-k', '--keep_uuid', dest='keep_uuid', action='store_true',
                        help='Keeps ID of old file even if the data is overwritten', default=False)
    ARGS = parser.parse_args()
    main()
