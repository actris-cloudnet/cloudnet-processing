#!venv/bin/python3
"""Master script for CloudnetPy backward processing."""
import os
import argparse
import configparser
import importlib
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.instruments import rpg2nc, ceilo2nc
from cloudnetpy import utils
process_utils = importlib.import_module("operational-processing").utils


class FilePaths:
    def __init__(self, site_name, dvec):
        self.site_name = site_name
        self.dvec = dvec
        self.year = _get_year(self.dvec)
        self.site_info = process_utils.read_site_info(site_name)
        self.config = self._read_conf()

    def _read_conf(self):
        return {'main': _read_config('main'),
                'site': _read_config(self.site_name)}

    def build_calibrated_file_name(self, instrument, makedir=True):
        folder = self.build_standard_path('calibrated', instrument)
        if makedir:
            os.makedirs(folder, exist_ok=True)
        identifier = self.config['site']['INSTRUMENTS'][instrument]
        return self._get_nc_name(folder, identifier)

    def build_standard_output_file_name(self, product='categorize', makedir=True):
        folder_id = 'processed' if product == 'categorize' else 'products'
        root = self.config['main']['PATH']['output']
        folder = '/'.join((root, self.site_name, folder_id, product, self.year))
        if makedir:
            os.makedirs(folder, exist_ok=True)
        return self._get_nc_name(folder, product)

    def build_standard_path(self, folder_id, instrument_type):
        direction = 'input' if folder_id == 'uncalibrated' else 'output'
        root = self.config['main']['PATH'][direction]
        instrument = self.config['site']['INSTRUMENTS'][instrument_type]
        return '/'.join((root, self.site_name, folder_id, instrument, self.year))

    def build_rpg_path(self):
        year, month, day = _split_date(self.dvec)
        root = self.config['main']['PATH']['input']
        return '/'.join((root, self.site_name, 'uncalibrated', 'rpg-fmcw-94',
                         f"Y{year}", f"M{month}", f"D{day}"))

    def build_mwr_file_name(self):
        if self.config['site']['INSTRUMENTS']['mwr'] == '':
            return self.build_calibrated_file_name('radar', makedir=False)
        raise NotImplementedError

    def _get_nc_name(self, folder, identifier):
        file_name = f"{self.dvec}_{self.site_name}_{identifier}.nc"
        return '/'.join((folder, file_name))


def main():
    start_date = process_utils.date_string_to_date(ARGS.start[0])
    end_date = process_utils.date_string_to_date(ARGS.stop[0])
    for date in utils.date_range(start_date, end_date):
        dvec = date.strftime("%Y%m%d")
        print('Date: ', dvec)
        obj = FilePaths(ARGS.site[0], dvec)
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
        try:
            _ = process_utils.find_file(rpg_path, '*.LV1')
        except FileNotFoundError:
            raise UncalibratedFileMissing("Can't find uncalibrated radar file.")
        if _is_good_to_process(output_file):
            print(f"Calibrating rpg-fmcw-94 cloud radar..")
            rpg2nc(rpg_path, output_file, obj.site_info)


def _process_lidar(obj):
    input_path = obj.build_standard_path('uncalibrated', 'lidar')
    try:
        input_file = process_utils.find_file(input_path, f"*{obj.dvec[3:]}*")
    except FileNotFoundError:
        raise UncalibratedFileMissing("Can't find uncalibrated lidar file.")
    output_file = obj.build_calibrated_file_name('lidar')
    if _is_good_to_process(output_file):
        print(f"Calibrating {obj.config['site']['INSTRUMENTS']['lidar']} lidar..")
        try:
            ceilo2nc(input_file, output_file, obj.site_info)
        except RuntimeError as error:
            raise error


def _process_categorize(obj):
    input_files = {key: obj.build_calibrated_file_name(key, makedir=False)
                   for key in ['radar', 'lidar', 'model']}
    input_files['mwr'] = obj.build_mwr_file_name()
    for file in input_files.values():
        if not os.path.isfile(file):
            raise CalibratedFileMissing
    output_file = obj.build_standard_output_file_name()
    if _is_good_to_process(output_file):
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
    if _is_good_to_process(output_file):
        try:
            print(f"Processing {product} product..")
            getattr(module, f"generate_{product_prefix}")(categorize_file, output_file)
        except ValueError:
            raise RuntimeError(f"Something went wrong with {product} processing.")


def _is_good_to_process(output_file):
    if ARGS.overwrite or not  os.path.isfile(output_file):
        return True
    return False


def _split_date(dvec):
    year = _get_year(dvec)
    month = _get_month(dvec)
    day = _get_day(dvec)
    return year, month, day


def _get_year(dvec):
    return str(dvec[:4])


def _get_month(dvec):
    return str(dvec[4:6])


def _get_day(dvec):
    return str(dvec[6:8])


def _read_config(conf_type):
    config = configparser.ConfigParser()
    config.read(f"config/{conf_type}.ini")
    return config


class UncalibratedFileMissing(Exception):
    pass


class CalibratedFileMissing(Exception):
    pass


class CategorizeFileMissing(Exception):
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Cloudnet data.')
    parser.add_argument('site', nargs='+', help='Site Name', choices=['bucharest'])
    parser.add_argument('start', nargs='+', type=str, metavar='YYYY-MM-DD', help='Starting date.')
    parser.add_argument('stop', nargs='+', type=str, metavar='YYYY-MM-DD', help='Stopping date.')
    parser.add_argument('-o', '--overwrite', dest='overwrite', action='store_true',
                        help='Overwrites any existing files', default=False)
    ARGS = parser.parse_args()
    main()
