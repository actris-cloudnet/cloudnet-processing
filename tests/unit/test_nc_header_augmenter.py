import netCDF4
import os
from data_processing import nc_header_augmenter as nca
import pytest


class TestMwr:

    date_str = '2020-10-23'
    site_name = 'bucharest'

    def test_standard_fix(self, mwr_file):
        original_filename = os.path.basename(mwr_file)
        uuid_volatile = None
        uuid = nca.fix_mwr_file(mwr_file, original_filename, self.date_str, self.site_name,
                                uuid_volatile)
        nc = netCDF4.Dataset(mwr_file)
        assert len(uuid) == 32
        assert nc.file_uuid == uuid
        assert nc.year == '2020'
        assert nc.month == '10'
        assert nc.day == '23'
        nc.close()

    def test_user_supplied_uuid(self, mwr_file):
        original_filename = os.path.basename(mwr_file)
        uuid_volatile = 'abc'
        uuid = nca.fix_mwr_file(mwr_file, original_filename, self.date_str, self.site_name,
                                uuid_volatile)
        nc = netCDF4.Dataset(mwr_file)
        assert uuid == uuid_volatile
        assert nc.file_uuid == uuid_volatile
        nc.close()

    def test_wrong_date(self, mwr_file):
        wrong_date = '2020-10-24'
        original_filename = os.path.basename(mwr_file)
        with pytest.raises(AssertionError):
            nca.fix_mwr_file(mwr_file, original_filename, wrong_date, self.site_name, None)

    def test_title(self, mwr_file):
        original_filename = os.path.basename(mwr_file)
        nca.fix_mwr_file(mwr_file, original_filename, self.date_str, self.site_name, None)
        nc = netCDF4.Dataset(mwr_file)
        assert nc.title == 'Mwr file from Bucharest'
        nc.close()


class TestModel:

    site_name = 'bucharest'

    def test_standard_fix(self, model_file):
        uuid = nca.fix_model_file(model_file, self.site_name, None)
        nc = netCDF4.Dataset(model_file)
        assert len(uuid) == 32
        assert nc.file_uuid == uuid
        assert nc.year == '2020'
        assert nc.month == '10'
        assert nc.day == '14'
        nc.close()

    def test_user_supplied_uuid(self, model_file):
        uuid_volatile = 'abc'
        uuid = nca.fix_model_file(model_file, self.site_name, uuid_volatile)
        nc = netCDF4.Dataset(model_file)
        assert uuid == uuid_volatile
        assert nc.file_uuid == uuid_volatile
        nc.close()

    def test_title(self, model_file):
        nca.fix_model_file(model_file, self.site_name, None)
        nc = netCDF4.Dataset(model_file)
        assert nc.title == 'Model file from Bucharest'
        nc.close()
