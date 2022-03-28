import netCDF4
from cloudnet_processing import concat_wrapper as lib


def test__validate_date_attributes(nc_file):
    nc = netCDF4.Dataset(nc_file)
    assert lib._validate_date_attributes(nc, ['2020', '05', '20'])
