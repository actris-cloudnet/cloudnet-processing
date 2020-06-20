import pytest
import netCDF4


@pytest.fixture(scope='session')
def nc_file(tmpdir_factory):
    file_name = tmpdir_factory.mktemp("data").join("file.nc")
    root_grp = netCDF4.Dataset(file_name, "w", format="NETCDF4_CLASSIC")
    root_grp.createDimension('time', 10)
    root_grp.createDimension('range', 5)
    root_grp.createDimension('other', 3)
    root_grp.year = '2020'
    root_grp.month = '05'
    root_grp.day = '20'
    root_grp.close()
    return file_name
