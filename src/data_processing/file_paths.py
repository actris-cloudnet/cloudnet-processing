import os
from typing import Tuple


class FilePaths:
    def __init__(self, date: str, config: dict, site_info: dict):
        self.site_name: str = site_info['id']
        self.date: str = date
        self.config: dict = config
        self.site_info: dict = site_info
        self._year: str = _get_year(self.date)

    def build_calibrated_file_name(self, instrument: str, makedir: bool = True) -> str:
        folder = self.build_standard_path('calibrated', instrument)
        if makedir:
            os.makedirs(folder, exist_ok=True)
        identifier = self.config['site']['INSTRUMENTS'][instrument]
        return self._get_nc_name(folder, identifier)

    def build_standard_output_file_name(self, product: str = 'categorize', makedir: bool = True) -> str:
        folder = self.build_standard_output_path(product)
        if makedir:
            os.makedirs(folder, exist_ok=True)
        return self._get_nc_name(folder, product)

    def build_standard_output_path(self, product: str = 'categorize') -> str:
        folder_id = 'processed' if product == 'categorize' else 'products'
        root = self.config['main']['PATH']['output']
        return os.path.join(root, self.site_name, folder_id, product, self._year)

    def build_standard_path(self, folder_id: str, instrument_type: str) -> str:
        direction = 'input' if folder_id == 'uncalibrated' else 'output'
        root = self.config['main']['PATH'][direction]
        instrument = self.config['site']['INSTRUMENTS'][instrument_type]
        return os.path.join(root, self.site_name, folder_id, instrument, self._year)

    def build_rpg_path(self) -> str:
        year, month, day = _split_date(self.date)
        root = self.config['main']['PATH']['input']
        prefix = os.path.join(root, self.site_name, 'uncalibrated', 'rpg-fmcw-94')
        path_option1 = os.path.join(prefix, year, month, day)
        path_option2 = os.path.join(prefix, f"Y{year}", f"M{month}", f"D{day}")
        if os.path.isdir(path_option1):
            return path_option1
        return path_option2

    def build_mwr_file_name(self) -> str:
        if self.config['site']['INSTRUMENTS']['mwr'] == '':
            return self.build_calibrated_file_name('radar', makedir=False)
        raise NotImplementedError

    def _get_nc_name(self, folder: str, identifier: str) -> str:
        file_name = f"{self.date}_{self.site_name}_{identifier}.nc"
        return os.path.join(folder, file_name)


def _split_date(date: str) -> Tuple[str, str, str]:
    year = _get_year(date)
    month = _get_month(date)
    day = _get_day(date)
    return year, month, day


def _get_year(date: str) -> str:
    return str(date[:4])


def _get_month(date: str) -> str:
    return str(date[4:6])


def _get_day(date: str) -> str:
    return str(date[6:8])
