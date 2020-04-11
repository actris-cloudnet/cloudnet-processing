import os
import importlib

process_utils = importlib.import_module("operational-processing").utils


class FilePaths:
    def __init__(self, dvec, config, site_info):
        self.site_name = site_info['id']
        self.dvec = dvec
        self.config = config
        self.site_info = site_info
        self._year = _get_year(self.dvec)

    def build_calibrated_file_name(self, instrument, makedir=True):
        folder = self.build_standard_path('calibrated', instrument)
        if makedir:
            os.makedirs(folder, exist_ok=True)
        identifier = self.config['site']['INSTRUMENTS'][instrument]
        return self._get_nc_name(folder, identifier)

    def build_standard_output_file_name(self, product='categorize', makedir=True):
        folder_id = 'processed' if product == 'categorize' else 'products'
        root = self.config['main']['PATH']['output']
        folder = '/'.join((root, self.site_name, folder_id, product, self._year))
        if makedir:
            os.makedirs(folder, exist_ok=True)
        return self._get_nc_name(folder, product)

    def build_standard_path(self, folder_id, instrument_type):
        direction = 'input' if folder_id == 'uncalibrated' else 'output'
        root = self.config['main']['PATH'][direction]
        instrument = self.config['site']['INSTRUMENTS'][instrument_type]
        return '/'.join((root, self.site_name, folder_id, instrument, self._year))

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
