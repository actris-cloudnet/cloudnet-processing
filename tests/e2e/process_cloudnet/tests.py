import os
import pytest
import netCDF4
from glob import glob


class TestCloudnetProcessing:

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.site = params['site']
        self.input = params['input']
        self.output = params['output']
        self.date = params['date'].replace('-', '')
        self.year = self.date[:4]

    def test_that_concatenates_lidar_files(self):
        file = '/'.join((self.input, self.site, 'uncalibrated/chm15k',
                         self.year, f"chm15k_{self.date}.nc"))
        assert os.path.isfile(file)

    def test_that_creates_calibrated_lidar_file(self):
        file = glob('/'.join((self.output, self.site, 'calibrated/chm15k',
                         self.year, f"{self.date}_{self.site}_chm15k_*.nc")))
        assert len(file) == 1
        file = file[0]
        assert os.path.isfile(file)
        assert netCDF4.Dataset(file).cloudnet_file_type == 'lidar'

    def test_that_creates_calibrated_radar_file(self):
        file = glob('/'.join((self.output, self.site, 'calibrated/rpg-fmcw-94',
                         self.year, f"{self.date}_{self.site}_rpg-fmcw-94_*.nc")))
        assert len(file) == 1
        file = file[0]
        assert os.path.isfile(file)
        assert netCDF4.Dataset(file).cloudnet_file_type == 'radar'

    def test_that_creates_categorize_file(self):
        file = glob('/'.join((self.output, self.site, 'processed/categorize',
                         self.year, f"{self.date}_{self.site}_categorize_*.nc")))
        assert len(file) == 1
        file = file[0]
        assert os.path.isfile(file)
        assert netCDF4.Dataset(file).cloudnet_file_type == 'categorize'

    def test_that_creates_product_files(self):
        products = ['iwc-Z-T-method', 'lwc-scaled-adiabatic', 'classification']
        file_types = ['iwc', 'lwc', 'classification']
        for product, file_type in zip(products, file_types):
            file = glob('/'.join((self.output, self.site, f"products/{product}",
                             self.year, f"{self.date}_{self.site}_{product}_*.nc")))
            assert len(file) == 1
            file = file[0]
            assert os.path.isfile(file)
            assert netCDF4.Dataset(file).cloudnet_file_type == file_type

    def test_that_PUTs_all_files_to_metadata_server(self):
        n_files = 7
        script_path = os.path.dirname(os.path.realpath(__file__))
        with open(f'{script_path}/md.log', 'r') as file:
            assert file.read().count('PUT') == n_files
