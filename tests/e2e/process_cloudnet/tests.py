import os
import pytest
import netCDF4
from glob import glob


@pytest.mark.first_run
@pytest.mark.append_data
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
        assert _read_file_type(file) == 'lidar'

    def test_that_creates_calibrated_radar_file(self):
        file = glob('/'.join((self.output, self.site, 'calibrated/rpg-fmcw-94',
                         self.year, f"{self.date}_{self.site}_rpg-fmcw-94_*.nc")))
        assert len(file) == 1
        file = file[0]
        assert os.path.isfile(file)
        assert _read_file_type(file) == 'radar'

    def test_that_creates_categorize_file(self):
        file = glob('/'.join((self.output, self.site, 'processed/categorize',
                         self.year, f"{self.date}_{self.site}_categorize_*.nc")))
        assert len(file) == 1
        file = file[0]
        assert os.path.isfile(file)
        assert _read_file_type(file) == 'categorize'

    def test_that_creates_product_files(self):
        products = ['iwc-Z-T-method', 'lwc-scaled-adiabatic', 'classification']
        file_types = ['iwc', 'lwc', 'classification', 'drizzle']
        for product, file_type in zip(products, file_types):
            file = glob('/'.join((self.output, self.site, f"products/{product}",
                             self.year, f"{self.date}_{self.site}_{product}_*.nc")))
            assert len(file) == 1
            file = file[0]
            assert os.path.isfile(file)
            assert _read_file_type(file) == file_type


@pytest.mark.new_version
@pytest.mark.append_fail
class TestCloudnetProcessingNewVersion:

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.site = params['site']
        self.input = params['input']
        self.output = params['output']
        self.date = params['date'].replace('-', '')
        self.year = self.date[:4]

    def test_that_exists_two_calibrated_lidar_files(self):
        files = glob('/'.join((self.output, self.site, 'calibrated/chm15k',
                               self.year, f"{self.date}_{self.site}_chm15k_*.nc")))
        assert len(files) == 2

    def test_that_exists_two_calibrated_radar_files(self):
        files = glob('/'.join((self.output, self.site, 'calibrated/rpg-fmcw-94',
                               self.year, f"{self.date}_{self.site}_rpg-fmcw-94_*.nc")))
        assert len(files) == 2

    def test_that_exists_two_categorize_files(self):
        files = glob('/'.join((self.output, self.site, 'processed/categorize',
                               self.year, f"{self.date}_{self.site}_categorize_*.nc")))
        assert len(files) == 2

    def test_that_exists_two_product_files_each(self):
        products = ['iwc-Z-T-method', 'lwc-scaled-adiabatic', 'classification']
        file_types = ['iwc', 'lwc', 'classification', 'drizzle']
        for product, file_type in zip(products, file_types):
            files = glob('/'.join((self.output, self.site, f"products/{product}",
                                   self.year, f"{self.date}_{self.site}_{product}_*.nc")))
            assert len(files) == 2


@pytest.mark.first_run
def test_that_PUTs_all_files_to_metadata_server():
    n_files = 7
    script_path = os.path.dirname(os.path.realpath(__file__))
    with open(f'{script_path}/md.log', 'r') as file:
        assert file.read().count('PUT') == n_files


@pytest.mark.append_data
def test_that_PUTs_updated_files_to_metadata_server():
    lines = []
    script_path = os.path.dirname(os.path.realpath(__file__))
    with open(f'{script_path}/md.log', 'r') as file:
        for line in file:
            lines.append(line)
    assert len(lines) == 14
    for n in range(7):
        first_sub = lines[n]
        second_sub = lines[n+7]
        ind = first_sub.index('PUT')
        assert first_sub[ind:] == second_sub[ind:]


@pytest.mark.new_version
@pytest.mark.append_fail
def test_that_PUTs_new_files_to_metadata_server():
    lines = []
    script_path = os.path.dirname(os.path.realpath(__file__))
    with open(f'{script_path}/md.log', 'r') as file:
        for line in file:
            lines.append(line)
    assert len(lines) == 21
    for n in range(7):
        first_sub = lines[n]
        third_sub = lines[n+14]
        ind = first_sub.index('PUT')
        assert first_sub[ind:] != third_sub[ind:]


def _read_file_type(file):
    nc = netCDF4.Dataset(file)
    value = nc.cloudnet_file_type
    nc.close()
    return value
