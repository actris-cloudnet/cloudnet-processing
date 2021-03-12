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


@pytest.fixture(scope='session')
def nc_file_with_pid(tmpdir_factory):
    file_name = tmpdir_factory.mktemp("data").join("file_pid.nc")
    root_grp = netCDF4.Dataset(file_name, "w", format="NETCDF4_CLASSIC")
    root_grp.createDimension('time', 10)
    root_grp.pid = 'w20930293029fj3'
    root_grp.close()
    return file_name


@pytest.fixture(scope='function')
def mwr_file(tmpdir_factory):
    file_name = tmpdir_factory.mktemp("data").join("201023.LWP.NC")
    root_grp = netCDF4.Dataset(file_name, "w", format="NETCDF3_CLASSIC")
    root_grp.createDimension('time', None)
    time = root_grp.createVariable('time', 'i4', ('time',))
    time[:] = [625190102, 625190101, 625190103, 625190104, 625190105] + 10*[627190105]
    time.units = 'seconds since 1.1.2001, 00:00:00'
    lwp = root_grp.createVariable('LWP_data', 'f4', ('time',))
    lwp[:] = 15*[1.2]
    lwp.units = 'kg m-2'
    root_grp.Conventions = 'CF-1.0'
    root_grp.close()
    return file_name


@pytest.fixture(scope='session')
def model_file(tmpdir_factory):
    file_name = tmpdir_factory.mktemp("data").join("xkljslfksef")
    root_grp = netCDF4.Dataset(file_name, "w", format="NETCDF3_CLASSIC")
    root_grp.createDimension('time', 25)
    time = root_grp.createVariable('time', 'f8', ('time',))
    time.units = 'hours since 2020-10-14 00:00:00 +00:00'
    root_grp.title = 'ECMWF single-site output over Bucharest'
    root_grp.location = ''
    root_grp.history = 'Model history'
    root_grp.Conventions = 'CF-1.0'
    root_grp.close()
    return file_name


@pytest.fixture(scope='session')
def bad_gdas1_file(tmpdir_factory):
    file_name = tmpdir_factory.mktemp("data").join("xkljslfksef")
    root_grp = netCDF4.Dataset(file_name, "w", format="NETCDF3_CLASSIC")
    root_grp.createDimension('time', 1)
    time = root_grp.createVariable('time', 'f8', ('time',))
    time.units = 'hours since 2020-10-14 00:00:00 +00:00'
    root_grp.title = 'GDAS1 single-site output over Bucharest'
    root_grp.location = ''
    root_grp.history = 'Model history'
    root_grp.Conventions = 'CF-1.0'
    root_grp.close()
    return file_name


@pytest.fixture(scope='session')
def halo_file(tmpdir_factory):
    file_name = tmpdir_factory.mktemp("data").join("20201014_fajdlfksfdjl")
    root_grp = netCDF4.Dataset(file_name, "w", format="NETCDF3_CLASSIC")
    root_grp.createDimension('time', 10)
    time = root_grp.createVariable('time', 'f8')
    time.units = 'hours since 2020-10-14 00:00:00 +00:00'
    root_grp.createDimension('range', 3)
    range = root_grp.createVariable('range', 'f8', ('range',))
    range[:] = [2, 4, 6]
    height_asl = root_grp.createVariable('height_asl', 'f8', ('range',))
    height_asl[:] = [1, 2, 3]
    root_grp.title = 'FMI HALO Doppler lidar'
    root_grp.location = 'Hyytiälä'
    root_grp.history = '28 Jan 2021 05:16:27 - Created by Antti Manninen <antti.manninen@fmi.fi>'
    root_grp.Conventions = 'CF-1.0'
    root_grp.close()
    return file_name
