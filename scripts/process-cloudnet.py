#!venv/bin/python3
"""Master script for CloudnetPy backward processing."""
import argparse
import configparser
import importlib
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.instruments import mira2nc, rpg2nc, ceilo2nc
from cloudnetpy.plotting import generate_figure
from cloudnetpy import utils
import os, sys
process_utils = importlib.import_module("operational-processing").utils


class ProcessingInfo:
    def __init__(self, site_name, dvec):
        self.site_name = site_name
        self.dvec = dvec
        self.site_info = process_utils.read_site_info(site_name)
        self.config = self._read_conf()

    def _read_conf(self):
        return {'main': _read_config('main'),
                'site': _read_config(self.site_name)}

    def build_calibrated_file_name(self, instrument):
        folder = self.build_standard_path('calibrated', instrument)
        os.makedirs(folder, exist_ok=True)
        identifier = self.config['site']['INSTRUMENTS'][instrument]
        return self._get_nc_name(folder, identifier)

    def build_uncalibrated_rpg_path(self):
        year, month, day = _split_date(self.dvec)
        root = self.config['main']['PATH']['input']
        return '/'.join((root, self.site_name, 'uncalibrated', 'rpg-fmcw-94',
                         f"Y{year}", f"M{month}", f"D{day}"))

    def build_standard_path(self, stage, instrument_type):
        direction = 'input' if stage == 'uncalibrated' else 'output'
        root = self.config['main']['PATH'][direction]
        instrument = self.config['site']['INSTRUMENTS'][instrument_type]
        year = _get_year(self.dvec)
        return '/'.join((root, self.site_name, stage, instrument, year))

    def _get_nc_name(self, folder, identifier):
        file_name = f"{self.dvec}_{self.site_name}_{identifier}.nc"
        return '/'.join((folder, file_name))


def main():

    start_date = process_utils.date_string_to_date(ARGS.start[0])
    end_date = process_utils.date_string_to_date(ARGS.stop[0])

    for date in utils.date_range(start_date, end_date):
        dvec = date.strftime("%Y%m%d")
        print('Date: ', dvec)

        obj = ProcessingInfo(ARGS.site[0], dvec)

        for processing_type in ('radar', 'lidar'):
            _run_processing(processing_type, obj)

        #for product in ('classification', 'iwc-Z-T-method',
        #                'lwc-scaled-adiabatic', 'drizzle'):
        #    try:
        #        _process_product(product, obj)
        #    except RuntimeError as error:
        #        print(error)
        print(' ')


def _run_processing(process_type, obj):
    module = importlib.import_module(__name__)
    try:
        getattr(module, f"_process_{process_type}")(obj)
    except RuntimeError as error:
        print(error)


def _process_radar(obj):
    output_file = obj.build_calibrated_file_name('radar')
    if obj.config['site']['INSTRUMENTS']['radar'] == 'rpg-fmcw-94':
        rpg_path = obj.build_uncalibrated_rpg_path()
        try:
            _ = process_utils.find_file(rpg_path, '*.LV1')
        except FileNotFoundError:
            raise RuntimeError('Abort: Missing uncalibrated rpg .LV1 files.')

        if _is_good_to_process(output_file):
            print(f"Calibrating rpg-fmcw-94 cloud radar..")
            rpg2nc(rpg_path, output_file, obj.site_info)


def _process_lidar(obj):
    input_path = obj.build_standard_path('uncalibrated', 'lidar')
    try:
        input_file = process_utils.find_file(input_path, f"*{obj.dvec[3:]}*")
    except FileNotFoundError:
        raise RuntimeError('Abort: Missing uncalibrated lidar file.')

    output_file = obj.build_calibrated_file_name('lidar')
    if _is_good_to_process(output_file):
        print(f"Calibrating {obj.config['site']['INSTRUMENTS']['lidar']} lidar..")
        try:
            ceilo2nc(input_file, output_file, obj.site_info)
        except RuntimeError as error:
            raise RuntimeError(f"Problem in ceilometer processing: {error}")


def _process_categorize(dvec):
    output_file = _build_categorize_file_name(dvec)
    if _is_good_to_process('categorize', output_file):
        try:
            input_files = {
                'radar': _find_calibrated_file('radar', dvec),
                'lidar': _find_calibrated_file('lidar', dvec),
                'mwr': _find_mwr_file(dvec),
                'model': _find_calibrated_file('model', dvec)}
        except FileNotFoundError as error:
            raise RuntimeError(f"Cannot process categorize file, missing input files: {error}")
        try:
            print(f"Processing categorize file..")
            generate_categorize(input_files, output_file)
        except RuntimeError as error:
            raise error
    image_name = _make_image_name(output_file)
    if _is_good_to_plot('categorize', image_name):
        print(f"Generating categorize quicklook..")
        fields = ['Z', 'v', 'ldr', 'width', 'v_sigma', 'beta', 'lwp']
        fields = ['Z', 'beta', 'lwp']
        generate_figure(output_file, fields, image_name=image_name,
                        show=False, max_y=7)


def _process_product(product, dvec):
    if not _should_we_process(product):
        return
    try:
        categorize_file = _find_categorize_file(dvec)
    except FileNotFoundError:
        raise RuntimeError(f"Failed to process {product}. Categorize file is missing.")
    output_file = _build_product_name(product, dvec)
    product_prefix = product.split('-')[0]
    module = importlib.import_module(f"cloudnetpy.products.{product_prefix}")
    if _is_good_to_process(product, output_file):
        print(f"Processing {product} product..")
        getattr(module, f"generate_{product_prefix}")(categorize_file, output_file)
    image_name = _make_image_name(output_file)
    if _is_good_to_plot(product, image_name):
        print(f"Generating {product} quicklook..")
        fields, max_y = _get_product_fields_in_plot(product_prefix)
        generate_figure(output_file, fields, image_name=image_name,
                        show=config.getboolean('MISC', 'show_plot'),
                        max_y=max_y)


def _get_product_fields_in_plot(product, max_y=10):
    if product == 'classification':
        fields = ['target_classification', 'detection_status']
    elif product == 'iwc':
        fields = ['iwc', 'iwc_error', 'iwc_retrieval_status']
    elif product == 'lwc':
        fields = ['lwc', 'lwc_error', 'lwc_retrieval_status']
        max_y = 8
    elif product == 'drizzle':
        fields = ['Do', 'mu', 'S']
        max_y = 4
    else:
        fields = []
    return fields, max_y


def _build_categorize_file_name(dvec):
    output_path = _find_categorize_path(dvec)
    return _get_nc_name(output_path, 'categorize', dvec)


def _build_product_name(product, dvec):
    output_path = _find_product_path(product, dvec)
    return _get_nc_name(output_path, product, dvec)


def _is_good_to_process(output_file):
    if ARGS.overwrite or not  os.path.isfile(output_file):
        return True
    return False


def _is_good_to_plot(process_type, image_name):
    #is_file = os.path.isfile(image_name)
    #quicklook_level = config.getint('QUICKLOOK_LEVEL', process_type)
    #plot_always = quicklook_level == 2
    #process_if_missing = quicklook_level == 1 and not is_file
    #return plot_always or process_if_missing
    return True


def _find_mwr_file(dvec):
    _, month, day = _split_date(dvec)
    prefix = _find_uncalibrated_path('mwr', dvec)
    hatpro_path = f"{prefix}{month}/{day}/"
    try:
        return process_utils.find_file(hatpro_path, f"*{dvec[2:]}*LWP*")
    except FileNotFoundError:
        if config['INSTRUMENTS']['radar'] == 'rpg-fmcw-94':
            return _find_calibrated_file('radar', dvec)
        raise FileNotFoundError('Missing MWR file')


def _find_calibrated_file(instrument, dvec, conf):
    file_path = _find_calibrated_path(instrument, dvec, conf)
    return process_utils.find_file(file_path, f"*{dvec}*.nc")


def _find_categorize_file(dvec):
    file_path = _find_categorize_path(dvec)
    return process_utils.find_file(file_path, f"*{dvec}*.nc")


def _find_product_file(product, dvec):
    file_path = _find_product_path(product, dvec)
    return process_utils.find_file(file_path, f"*{dvec}*.nc")


def _find_categorize_path(dvec):
    year = _get_year(dvec)
    categorize_path = f"{OUTPUT_ROOT}/processed/categorize/{year}/"
    if not os.path.exists(categorize_path):
        os.makedirs(categorize_path)
    return categorize_path


def _find_product_path(product, dvec):
    year = _get_year(dvec)
    product_path = f"{OUTPUT_ROOT}/products/{product}/{year}/"
    if not os.path.exists(product_path):
        os.makedirs(product_path)
    return product_path


def _make_image_name(output_file):
    return output_file.replace('.nc', '.png')


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


def _get_input_data_paths(conf, site_name):
    def _get_root(root, stage, identifier):
        path = conf['main']['PATH'][root]
        instrument_type = conf['site']['INSTRUMENTS'][identifier]
        return '/'.join((path, site_name, stage, instrument_type))

    return {'radar': _get_root('input', 'uncalibrated', 'radar'),
            'lidar': _get_root('input', 'uncalibrated', 'lidar'),
            'mwr': _get_root('input', 'uncalibrated', 'mwr'),
            'model': _get_root('output', 'calibrated', 'model')}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Cloudnet data.')
    parser.add_argument('site', nargs='+', help='Site Name', choices=['bucharest'])
    parser.add_argument('start', nargs='+', type=str, metavar='YYYY-MM-DD', help='Starting date.')
    parser.add_argument('stop', nargs='+', type=str, metavar='YYYY-MM-DD', help='Stopping date.')
    parser.add_argument('-o', '--overwrite', dest='overwrite', action='store_true',
                        help='Overwrites any existing files', default=False)
    ARGS = parser.parse_args()
    main()
