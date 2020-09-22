#!/usr/bin/env python3
"""Master script for CloudnetPy backward processing."""
import os
import argparse
from typing import Tuple, Union
import importlib
import tempfile
import shutil
import cloudnetpy.utils
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.instruments import rpg2nc, ceilo2nc
from requests import HTTPError
from data_processing import utils
from data_processing.file_paths import FilePaths
from data_processing.metadata_api import MetadataApi

FILE_EXISTS_AND_NOT_CHANGED = 409
TEMP_DIR = tempfile.TemporaryDirectory()
PRODUCTS = ('classification', 'iwc-Z-T-method', 'lwc-scaled-adiabatic', 'drizzle')


def main():
    """The main function."""

    config = utils.read_conf(ARGS)
    site_name = ARGS.site[0]
    site_info = utils.read_site_info(site_name)

    start_date = utils.date_string_to_date(ARGS.start)
    stop_date = utils.date_string_to_date(ARGS.stop)

    md_api = MetadataApi(config['main']['METADATASERVER']['url'])

    for date in cloudnetpy.utils.date_range(start_date, stop_date):
        date_str = date.strftime("%Y%m%d")
        print('Date: ', date_str)
        file_paths = FilePaths(date_str, config, site_info)

        processed_files = {
            'lidar': '',
            'radar': '',
            'categorize': ''
        }
        for processing_type in processed_files.keys():
            try:
                file_to_append = _find_volatile_file(processing_type, file_paths)
                res = _process_level1(processing_type, file_paths, processed_files, file_to_append)
                processed_files[processing_type] = _archive_file(*res, md_api, file_to_append)
            except (UncalibratedFileMissing, CalibratedFileMissing, VolatileFileError, RuntimeError,
                    ValueError, IndexError, TypeError, NotImplementedError) as error:
                print(error)

        for product in PRODUCTS:
            try:
                file_to_append = _find_volatile_file(product, file_paths)
                res = _process_level2(product, file_paths, processed_files, file_to_append)
                _ = _archive_file(*res, md_api, file_to_append)
            except (CategorizeFileMissing, RuntimeError, ValueError, IndexError, TypeError, VolatileFileError) as error:
                print(error)
    TEMP_DIR.cleanup()


def _process_level1(*args) -> Tuple[str, str, str]:
    _print_info(args[0], args[3])
    module = importlib.import_module(__name__)
    res = getattr(module, f"_process_{args[0]}")(*args[1:])
    print("done")
    return res


def _process_radar(file_paths: FilePaths, _, file_to_append: Union[str, None]) -> Tuple[str, str, str]:
    output_file_temp, output_file = _build_output_file_names('radar', file_paths, file_to_append)
    if file_paths.config['site']['INSTRUMENTS']['radar'] == 'rpg-fmcw-94':
        rpg_path = file_paths.build_rpg_path()
        _ = _find_uncalibrated_file(rpg_path, '*.LV1')
        uuid = rpg2nc(rpg_path, output_file_temp, file_paths.site_info, keep_uuid=isinstance(file_to_append, str))
        return output_file_temp, output_file, uuid
    raise NotImplementedError


def _process_lidar(file_paths: FilePaths, _, file_to_append: Union[str, None]) -> Tuple[str, str, str]:
    output_file_temp, output_file = _build_output_file_names('lidar', file_paths, file_to_append)
    input_path = file_paths.build_standard_path('uncalibrated', 'lidar')
    input_file = _find_uncalibrated_file(input_path, f"*{file_paths.date[3:]}*")
    uuid = ceilo2nc(input_file, output_file_temp, file_paths.site_info, keep_uuid=isinstance(file_to_append, str))
    return output_file_temp, output_file, uuid


def _process_categorize(file_paths: FilePaths, processed_files: dict,
                        file_to_append: Union[str, None]) -> Tuple[str, str, str]:
    input_files = processed_files.copy()
    input_files['model'] = file_paths.build_calibrated_file_name('model', makedir=False)
    input_files['mwr'] = input_files['radar']
    del input_files['categorize']
    for file in input_files.values():
        if not os.path.isfile(file):
            raise CalibratedFileMissing
    output_file_temp, output_file = _build_output_file_names('categorize', file_paths, file_to_append)
    uuid = generate_categorize(input_files, output_file_temp, keep_uuid=isinstance(file_to_append, str))
    return output_file_temp, output_file, uuid


def _process_level2(product: str, file_paths: FilePaths, processed_files: dict,
                    file_to_append: Union[str, None]) -> Tuple[str, str, str]:
    _print_info(product, file_to_append)
    categorize_file = processed_files['categorize']
    if not os.path.isfile(categorize_file):
        raise CategorizeFileMissing
    output_file_temp, output_file = _build_output_file_names(product, file_paths, file_to_append)
    product_prefix = product.split('-')[0]
    module = importlib.import_module(f"cloudnetpy.products.{product_prefix}")
    fun = getattr(module, f"generate_{product_prefix}")
    uuid = fun(categorize_file, output_file_temp, keep_uuid=isinstance(file_to_append, str))
    print('done')
    return output_file_temp, output_file, uuid


def _build_output_file_names(cloudnet_file_type: str, file_paths: FilePaths,
                             file_to_append: Union[str, None]) -> Tuple[str, str]:
    """Creates temporary filename and actual filename (without uuid-suffix).

    Args:
        cloudnet_file_type (str): E.g. 'radar', 'categorize', 'iwc', etc.
        file_paths (FilePaths): FilePaths instance.
        file_to_append (str / None): Existing volatile file to be rewritten.

    Returns:
        tuple: A two element tuple containing:
            - output_file_temp (str): Temporary filename. New file is initially written here.
            - output_file (str): Actual filename we want to have in the end.

    Notes:
        - In case of a volatile file, we can not write into temp directory. Instead we must
        overwrite the existing file. We also know the final filename because the name will not change.
        - In case of a new file version, we first create the file in temp folder (output_file_temp).
        Then later, we add UUID in filename and finally move the file in the correct folder (output_file).

    """
    if file_to_append:
        output_file_temp = file_to_append
        output_file = file_to_append
    else:
        if cloudnet_file_type == 'categorize' or cloudnet_file_type in PRODUCTS:
            output_file = file_paths.build_standard_output_file_name(cloudnet_file_type)
        else:
            output_file = file_paths.build_calibrated_file_name(cloudnet_file_type)
        output_file_temp = utils.replace_path(output_file, TEMP_DIR.name)
    return output_file_temp, output_file


def _archive_file(output_file_temp: str, output_file: str, uuid: str, md_api: MetadataApi,
                  file_to_append: Union[str, None]) -> str:
    if file_to_append:
        # We have only rewritten the existing volatile file (with already correct name):
        output_file = output_file_temp
    else:
        # We have file in temp folder. Now add uuid-to filename and move to correct folder:
        output_file = _rename_and_move_to_correct_folder(output_file_temp, output_file, uuid)
    if not ARGS.no_api:
        try:
            md_api.put(uuid, output_file)
        except HTTPError as error:
            print(error)
            os.remove(output_file)
            if error.response.status_code != FILE_EXISTS_AND_NOT_CHANGED:  # Something badly wrong in the portal
                raise error
    return output_file


def _rename_and_move_to_correct_folder(temp_filename: str, true_filename: str, uuid: str) -> str:
    temp_filename = utils.add_uuid_to_filename(uuid, temp_filename)
    true_filename = utils.replace_path(temp_filename, os.path.dirname(true_filename))
    shutil.move(temp_filename, true_filename)
    return true_filename


def _find_volatile_file(cloudnet_file_type: str, file_paths: FilePaths) -> Union[str, None]:
    existing_files = _get_cloudnet_files(cloudnet_file_type, file_paths)
    n_files = len(existing_files)
    if not ARGS.new_version:
        if n_files > 1 or (n_files == 1 and not utils.is_volatile_file(existing_files[0])):
            raise VolatileFileError
        if n_files == 1 and utils.is_volatile_file(existing_files[0]):
            return existing_files[0]
    return None


def _get_cloudnet_files(processing_type: str, file_paths: FilePaths) -> list:
    if processing_type in ('radar', 'lidar'):
        path = file_paths.build_standard_path('calibrated', processing_type)
    else:
        path = file_paths.build_standard_output_path(processing_type)
    return utils.list_files(path, f"{file_paths.date}*.nc")


def _find_uncalibrated_file(path: str, pattern: str) -> str:
    try:
        return utils.find_file(path, pattern)
    except FileNotFoundError:
        raise UncalibratedFileMissing()


def _print_info(cloudnet_file_type: str, file_to_append: Union[str, None]) -> None:
    if file_to_append:
        prefix = 'Appending to existing volatile'
    else:
        prefix = 'Creating new'
    print(f"{prefix} {cloudnet_file_type} file.. ", end='')


class VolatileFileError(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = "Can't update a stable file"
        super().__init__(self.message)


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
