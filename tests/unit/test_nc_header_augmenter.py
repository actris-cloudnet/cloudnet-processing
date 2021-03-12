import netCDF4
import os
from data_processing import nc_header_augmenter as nca
from data_processing.utils import MiscError
import pytest
import numpy as np


class TestMwr:

    data = {
        'site_name': 'bucharest',
        'date': '2020-10-23',
        'uuid': None,
        'instrument': 'hatpro',
        'model': None
    }

    def test_standard_fix(self, mwr_file):
        self.data['full_path'] = mwr_file
        self.data['original_filename'] = os.path.basename(mwr_file)
        uuid = nca.harmonize_nc_file(self.data)
        nc = netCDF4.Dataset(mwr_file)
        assert len(nc.variables['time'][:]) == 5
        assert len(uuid) == 32
        assert nc.file_uuid == uuid
        assert nc.year == '2020'
        assert nc.month == '10'
        assert nc.day == '23'
        assert nc.location == 'Bucharest'
        assert nc.title == 'Mwr file from Bucharest'
        assert nc.Conventions == 'CF-1.0'
        nc.close()

    def test_user_supplied_uuid(self, mwr_file):
        uuid_volatile = 'abc'
        self.data['full_path'] = mwr_file
        self.data['original_filename'] = os.path.basename(mwr_file)
        self.data['uuid'] = uuid_volatile
        uuid = nca.harmonize_nc_file(self.data)
        nc = netCDF4.Dataset(mwr_file)
        assert uuid == uuid_volatile
        assert nc.file_uuid == uuid_volatile
        nc.close()

    def test_wrong_date(self, mwr_file):
        self.data['full_path'] = mwr_file
        self.data['original_filename'] = os.path.basename(mwr_file)
        self.data['date'] = '2020-10-24'
        with pytest.raises(RuntimeError):
            nca.harmonize_nc_file(self.data)


class TestModel:

    data = {
        'site_name': 'bucharest',
        'date': '2020-10-14',
        'uuid': None,
        'instrument': None,
        'model': 'ecmwf'
    }

    def test_model_fix(self, model_file):
        self.data['full_path'] = model_file
        self.data['original_filename'] = os.path.basename(model_file)
        uuid = nca.harmonize_nc_file(self.data)
        nc = netCDF4.Dataset(model_file)
        assert len(uuid) == 32
        assert nc.file_uuid == uuid
        assert nc.year == '2020'
        assert nc.month == '10'
        assert nc.day == '14'
        assert nc.title == 'Model file from Bucharest'
        assert nc.Conventions == 'CF-1.7'
        nc.close()

    def test_bad_model_file(self, bad_gdas1_file):
        self.data['full_path'] = bad_gdas1_file
        self.data['original_filename'] = os.path.basename(bad_gdas1_file)
        self.data['model'] = 'gdas1'
        with pytest.raises(MiscError):
            nca.harmonize_nc_file(self.data)


class TestHalo:

    data = {
        'site_name': 'hyytiala',
        'date': '2020-10-14',
        'uuid': None,
        'instrument': 'halo-doppler-lidar',
    }

    def test_halo_fix(self, halo_file):
        self.data['full_path'] = halo_file
        self.data['original_filename'] = os.path.basename(halo_file)
        uuid = nca.harmonize_nc_file(self.data)
        nc = netCDF4.Dataset(halo_file)
        assert len(uuid) == 32
        assert nc.file_uuid == uuid
        assert nc.year == '2020'
        assert nc.month == '10'
        assert nc.day == '14'
        assert nc.title == 'Lidar file from Hyytiälä'
        assert nc.Conventions == 'CF-1.7'
        assert 'height' in nc.variables
        nc.close()


@pytest.mark.parametrize('arg, expected', [
    ('seconds from 2020-01-15 00:00:00', (2020, 1, 15)),
    ('seconds from 2020.1.15', (2020, 1, 15)),
    ('seconds from bad-times 00:00:00', (2001, 1, 1)),
    ('blabla', (2001, 1, 1)),
])
def test_get_epoch(arg, expected):
    assert nca._get_epoch(arg) == expected


def test_harmonize_hatpro_file(mwr_file):
    nc = netCDF4.Dataset(mwr_file, 'r+')
    nc = nca._harmonize_hatpro_file(nc)
    assert 'LWP' in nc.variables
    assert nc.variables['LWP'].units == 'g m-2'
    time = nc.variables['time'][:]
    assert np.all(np.diff(time) >= 0)
    nc.close()
