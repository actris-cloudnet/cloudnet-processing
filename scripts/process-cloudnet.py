#!/usr/bin/env python3
"""Master script for CloudnetPy backward processing."""
import os
import argparse
import importlib
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.instruments import rpg2nc, ceilo2nc
from cloudnetpy import utils
from requests import HTTPError

from data_processing import metadata_api, file_paths
import data_processing.utils as process_utils

FILE_EXISTS_AND_NOT_CHANGED = 409


def main():
    """The main function."""

    config = process_utils.read_conf(ARGS)
    site_name = ARGS.site[0]
    site_info = process_utils.read_site_info(site_name)

    start_date = process_utils.date_string_to_date(ARGS.start)
    stop_date = process_utils.date_string_to_date(ARGS.stop)

    md_api = metadata_api.MetadataApi(config['main']['METADATASERVER']['url'])

    for date in utils.date_range(start_date, stop_date):
        dvec = date.strftime("%Y%m%d")
        print('Date: ', dvec)
        obj = file_paths.FilePaths(dvec, config, site_info)

        processed_files = {
            'lidar': '',
            'radar': '',
            'categorize': ''
        }

        for processing_type in processed_files.keys():
            try:
                output_file, uuid = _process_level1(processing_type, obj, processed_files)
                output_file = _rename_output_file(uuid, output_file)
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
            try:
                output_file, uuid = _process_level2(product, obj, processed_files)
                output_file = _rename_output_file(uuid, output_file)
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


def _process_level1(process_type, obj, processed_files):
    module = importlib.import_module(__name__)
    return getattr(module, f"_process_{process_type}")(obj, processed_files)


def _process_radar(obj, _):
    output_file = obj.build_calibrated_file_name('radar')
    if obj.config['site']['INSTRUMENTS']['radar'] == 'rpg-fmcw-94':
        rpg_path = obj.build_rpg_path()
        _ = _find_input_file(rpg_path, '*.LV1')
        if _is_writable(output_file):
            print("Calibrating rpg-fmcw-94 cloud radar..")
            return output_file, rpg2nc(rpg_path, output_file, obj.site_info,
                                       keep_uuid=ARGS.keep_uuid)
    raise NotImplementedError


def _process_lidar(obj, _):
    input_path = obj.build_standard_path('uncalibrated', 'lidar')
    input_file = _find_input_file(input_path, f"*{obj.dvec[3:]}*")
    output_file = obj.build_calibrated_file_name('lidar')
    if _is_writable(output_file):
        print(f"Calibrating {obj.config['site']['INSTRUMENTS']['lidar']} lidar..")
        try:
            return output_file, ceilo2nc(input_file, output_file, obj.site_info,
                                         keep_uuid=ARGS.keep_uuid)
        except RuntimeError as error:
            raise error
    raise NotWritableFile


def _find_input_file(path, pattern):
    try:
        return process_utils.find_file(path, pattern)
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
    if _is_writable(output_file):
        try:
            print("Processing categorize file..")
            return output_file, generate_categorize(input_files, output_file,
                                                    keep_uuid=ARGS.keep_uuid)
        except RuntimeError as error:
            raise error
    raise NotWritableFile


def _process_level2(product, obj, processed_files):
    categorize_file = processed_files['categorize']
    if not os.path.isfile(categorize_file):
        raise CategorizeFileMissing
    output_file = obj.build_standard_output_file_name(product=product)
    product_prefix = product.split('-')[0]
    module = importlib.import_module(f"cloudnetpy.products.{product_prefix}")
    if _is_writable(output_file):
        try:
            print(f"Processing {product} product..")
            fun = getattr(module, f"generate_{product_prefix}")
            return output_file, fun(categorize_file, output_file,
                                    keep_uuid=ARGS.keep_uuid)
        except ValueError:
            raise RuntimeError(f"Something went wrong with {product} processing.")
    raise NotWritableFile


def _is_writable(output_file):
    if ARGS.overwrite or not os.path.isfile(output_file):
        return True
    return False


def _rename_output_file(uuid: str, output_file: str) -> str:
    suffix = '_' + uuid[:4]
    path, extension = os.path.splitext(output_file)
    new_output_file = f"{path}{suffix}{extension}"
    os.rename(output_file, new_output_file)
    return new_output_file


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
                        default=process_utils.get_date_from_past(7))
    parser.add_argument('--stop', type=str, metavar='YYYY-MM-DD',
                        help='Stopping date. Default is current day - 1.',
                        default=process_utils.get_date_from_past(1))
    parser.add_argument('--input', type=str, metavar='/FOO/BAR',
                        help='Input folder path. Overrides config/main.ini value.')
    parser.add_argument('--output', type=str, metavar='/FOO/BAR',
                        help='Output folder path. Overrides config/main.ini value.')
    parser.add_argument('-o', '--overwrite', dest='overwrite', action='store_true',
                        help='Overwrite data in existing files', default=False)
    parser.add_argument('-k', '--keep_uuid', dest='keep_uuid', action='store_true',
                        help='Keep ID of old file even if the data is overwritten', default=True)
    parser.add_argument('-na', '--no-api', dest='no_api', action='store_true',
                        help='Disable API calls. Useful for testing.', default=False)
    ARGS = parser.parse_args()
    main()
