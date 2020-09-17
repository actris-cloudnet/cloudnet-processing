#!/usr/bin/env python3
"""Master script for CloudnetPy backward processing."""
import os
import argparse
import importlib
import tempfile
import cloudnetpy.utils
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.instruments import rpg2nc, ceilo2nc
from requests import HTTPError

from data_processing import metadata_api, file_paths, utils

FILE_EXISTS_AND_NOT_CHANGED = 409

TEMP_DIR = tempfile.TemporaryDirectory()


def main():
    """The main function."""

    config = utils.read_conf(ARGS)
    site_name = ARGS.site[0]
    site_info = utils.read_site_info(site_name)

    start_date = utils.date_string_to_date(ARGS.start)
    stop_date = utils.date_string_to_date(ARGS.stop)

    md_api = metadata_api.MetadataApi(config['main']['METADATASERVER']['url'])

    for date in cloudnetpy.utils.date_range(start_date, stop_date):
        date_str = date.strftime("%Y%m%d")
        print('Date: ', date_str)
        obj = file_paths.FilePaths(date_str, config, site_info)

        processed_files = {
            'lidar': '',
            'radar': '',
            'categorize': ''
        }

        for processing_type in processed_files.keys():

            n_files = _count_cloudnet_files(processing_type, obj)

            try:
                output_file_temp, output_file, uuid = _process_level1(processing_type, obj, processed_files)
                output_file = _rename_and_move_to_correct_folder(output_file_temp, output_file, uuid)
                if not ARGS.no_api:
                    md_api.put(uuid, output_file)
                processed_files[processing_type] = output_file
            except (UncalibratedFileMissing, CalibratedFileMissing, RuntimeError,
                    ValueError, IndexError, TypeError, NotImplementedError,
                    NotWritableFile) as error:
                print(error)
            except HTTPError as error:
                print(error)
                os.remove(output_file)
                if error.response.status_code != FILE_EXISTS_AND_NOT_CHANGED:
                    raise error

        for product in ('classification', 'iwc-Z-T-method', 'lwc-scaled-adiabatic', 'drizzle'):

            n_files = _count_cloudnet_files(product, obj)

            try:
                output_file_temp, output_file, uuid = _process_level2(product, obj, processed_files)
                output_file = _rename_and_move_to_correct_folder(output_file_temp, output_file, uuid)
                if not ARGS.no_api:
                    md_api.put(uuid, output_file)
            except (CategorizeFileMissing, RuntimeError, ValueError,
                    IndexError, NotWritableFile) as error:
                print(error)
            except HTTPError as error:
                print(error)
                os.remove(output_file)
                if error.response.status_code != FILE_EXISTS_AND_NOT_CHANGED:
                    raise error
        print(' ')
    TEMP_DIR.cleanup()


def _count_cloudnet_files(processing_type, obj):
    if processing_type in ('radar', 'lidar'):
        path = obj.build_standard_path('calibrated', processing_type)
    else:
        path = obj.build_standard_output_path(processing_type)
    return utils.count_nc_files_for_date(path, obj.dvec)


def _rename_and_move_to_correct_folder(temp_filename: str, true_filename: str, uuid: str) -> str:
    temp_filename = utils.add_uuid_to_filename(uuid, temp_filename)
    true_filename = _replace_path(temp_filename, os.path.dirname(true_filename))
    os.rename(temp_filename, true_filename)
    return true_filename


def _replace_path(filename: str, new_path: str) -> str:
    return filename.replace(os.path.dirname(filename), new_path)


def _process_level1(process_type, obj, processed_files):
    module = importlib.import_module(__name__)
    return getattr(module, f"_process_{process_type}")(obj, processed_files)


def _process_radar(obj, _):
    output_file = obj.build_calibrated_file_name('radar')
    output_file_temp = _replace_path(output_file, TEMP_DIR.name)
    if obj.config['site']['INSTRUMENTS']['radar'] == 'rpg-fmcw-94':
        rpg_path = obj.build_rpg_path()
        _ = _find_input_file(rpg_path, '*.LV1')
        print("Calibrating rpg-fmcw-94 cloud radar..")
        uuid = rpg2nc(rpg_path, output_file_temp, obj.site_info)
        return output_file_temp, output_file, uuid
    raise NotImplementedError


def _process_lidar(obj, _):
    input_path = obj.build_standard_path('uncalibrated', 'lidar')
    input_file = _find_input_file(input_path, f"*{obj.dvec[3:]}*")
    output_file = obj.build_calibrated_file_name('lidar')
    output_file_temp = _replace_path(output_file, TEMP_DIR.name)
    print(f"Calibrating {obj.config['site']['INSTRUMENTS']['lidar']} lidar..")
    try:
        uuid = ceilo2nc(input_file, output_file_temp, obj.site_info)
        return output_file_temp, output_file, uuid
    except RuntimeError as error:
        raise error


def _find_input_file(path, pattern):
    try:
        return utils.find_file(path, pattern)
    except FileNotFoundError:
        raise UncalibratedFileMissing()


def _process_categorize(obj, processed_files):
    input_files = processed_files.copy()
    input_files['model'] = obj.build_calibrated_file_name('model', makedir=False)
    input_files['mwr'] = input_files['radar']
    del input_files['categorize']
    for file in input_files.values():
        if not os.path.isfile(file):
            raise CalibratedFileMissing
    output_file = obj.build_standard_output_file_name()
    output_file_temp = _replace_path(output_file, TEMP_DIR.name)
    try:
        print("Processing categorize file..")
        uuid = generate_categorize(input_files, output_file_temp)
        return output_file_temp, output_file, uuid
    except RuntimeError as error:
        raise error


def _process_level2(product, obj, processed_files):
    categorize_file = processed_files['categorize']
    if not os.path.isfile(categorize_file):
        raise CategorizeFileMissing
    output_file = obj.build_standard_output_file_name(product=product)
    output_file_temp = _replace_path(output_file, TEMP_DIR.name)
    product_prefix = product.split('-')[0]
    module = importlib.import_module(f"cloudnetpy.products.{product_prefix}")
    try:
        print(f"Processing {product} product..")
        fun = getattr(module, f"generate_{product_prefix}")
        uuid = fun(categorize_file, output_file_temp)
        return output_file_temp, output_file, uuid
    except ValueError:
        raise RuntimeError(f"Something went wrong with {product} processing.")


class UncalibratedFileMissing(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'Uncalibrated file missing'
        super().__init__(self.message)


class CalibratedFileMissing(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'Calibrated file missing'
        super().__init__(self.message)


class CategorizeFileMissing(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'Categorize file missing'
        super().__init__(self.message)


class NotWritableFile(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'File not writable'
        super().__init__(self.message)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Cloudnet data.')
    parser.add_argument('site', nargs='+', help='Site Name',
                        choices=['bucharest', 'norunda', 'granada', 'mace-head'])
    parser.add_argument('--config-dir', type=str, metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')
    parser.add_argument('--start', type=str, metavar='YYYY-MM-DD',
                        help='Starting date. Default is current day - 7.',
                        default=utils.get_date_from_past(7))
    parser.add_argument('--stop', type=str, metavar='YYYY-MM-DD',
                        help='Stopping date. Default is current day - 1.',
                        default=utils.get_date_from_past(1))
    parser.add_argument('--input', type=str, metavar='/FOO/BAR',
                        help='Input folder path. Overrides config/main.ini value.')
    parser.add_argument('--output', type=str, metavar='/FOO/BAR',
                        help='Output folder path. Overrides config/main.ini value.')
    parser.add_argument('-na', '--no-api', dest='no_api', action='store_true',
                        help='Disable API calls. Useful for testing.', default=False)
    parser.add_argument('--new-version', dest='new_version', action='store_true',
                        help='Process new version.', default=False)
    ARGS = parser.parse_args()
    main()
