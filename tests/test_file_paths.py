import pytest
import configparser


utils = __import__('operational-processing').utils
file_paths = __import__('operational-processing').file_paths
FilePaths = file_paths.FilePaths


@pytest.fixture(scope='session')
def file_paths_bucharest():
    conf_main = configparser.RawConfigParser()
    section = 'PATH'
    conf_main.add_section(section)
    conf_main.set(section, 'input', '/my/input')
    conf_main.set(section, 'output', '/my/output')
    conf_site = configparser.RawConfigParser()
    section = 'INSTRUMENTS'
    conf_site.add_section(section)
    conf_site.set(section, 'lidar', 'chm15k')
    conf_site.set(section, 'radar', 'rpg-fmcw-94')
    conf_site.set(section, 'model', 'ecmwf')
    conf_site.set(section, 'mwr', '')
    config = {'main': conf_main, 'site': conf_site}
    site_name = 'bucharest'
    dvec = '20200101'
    site_info = utils.read_site_info(site_name)
    return FilePaths(dvec, config, site_info)


class TestBucharestPaths:

    @pytest.fixture(autouse=True)
    def _request_google_page(self, file_paths_bucharest):
        self.obj = file_paths_bucharest

    def test_build_calibrated_file_name(self):
        expected = '/my/output/bucharest/calibrated/rpg-fmcw-94/2020/20200101_bucharest_rpg-fmcw-94.nc'
        result = self.obj.build_calibrated_file_name('radar', makedir=False)
        assert result == expected

    def test_build_standard_output_file_name(self):
        expected = '/my/output/bucharest/processed/categorize/2020/20200101_bucharest_categorize.nc'
        assert self.obj.build_standard_output_file_name(makedir=False) == expected
        expected = '/my/output/bucharest/products/classification/2020/20200101_bucharest_classification.nc'
        assert self.obj.build_standard_output_file_name('classification', makedir=False) == expected

    @pytest.mark.parametrize("a, b, res", [
        ('uncalibrated', 'radar', '/my/input/bucharest/uncalibrated/rpg-fmcw-94/2020'),
        ('uncalibrated', 'lidar', '/my/input/bucharest/uncalibrated/chm15k/2020'),
        ('uncalibrated', 'model', '/my/input/bucharest/uncalibrated/ecmwf/2020'),
        ('calibrated', 'radar', '/my/output/bucharest/calibrated/rpg-fmcw-94/2020'),
        ('calibrated', 'lidar', '/my/output/bucharest/calibrated/chm15k/2020'),
        ('calibrated', 'model', '/my/output/bucharest/calibrated/ecmwf/2020'),
    ])
    def test_build_standard_path(self, a, b, res):
        assert self.obj.build_standard_path(a, b) == res

    def test_build_rpg_path(self):
        assert self.obj.build_rpg_path() == '/my/input/bucharest/uncalibrated/rpg-fmcw-94/Y2020/M01/D01'

    def test_build_mwr_file_name(self):
        expected = '/my/output/bucharest/calibrated/rpg-fmcw-94/2020/20200101_bucharest_rpg-fmcw-94.nc'
        assert self.obj.build_mwr_file_name() == expected

    def test_get_nc_name(self):
        assert self.obj._get_nc_name('/my/folder', 'ecmwf') == '/my/folder/20200101_bucharest_ecmwf.nc'


def test_split_date():
    assert file_paths._split_date('20200414') == ('2020', '04', '14')
