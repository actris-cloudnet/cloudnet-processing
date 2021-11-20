import shutil
from tempfile import NamedTemporaryFile
import datetime
import netCDF4
import cloudnetpy.utils
from cloudnetpy import output
from cloudnetpy.utils import get_uuid, get_time, seconds2date
from cloudnetpy.metadata import COMMON_ATTRIBUTES
from cloudnetpy.instruments import instruments
from data_processing import utils
from data_processing.utils import MiscError



def fix_legacy_file(legacy_file_full_path: str, target_full_path: str) -> str:
    """Fix legacy netCDF file."""
    nc_legacy = netCDF4.Dataset(legacy_file_full_path, 'r')
    nc = netCDF4.Dataset(target_full_path, 'w', format='NETCDF4_CLASSIC')
    legacy = LegacyNc(nc_legacy, nc, {})
    legacy.copy_file_contents()
    uuid = legacy.add_uuid()
    legacy.add_history()
    legacy.close()
    return uuid


def harmonize_model_file(data: dict) -> str:
    """Harmonizes model netCDF file."""
    temp_file = NamedTemporaryFile()
    nc_raw = netCDF4.Dataset(data['full_path'], 'r')
    nc = netCDF4.Dataset(temp_file.name, 'w', format='NETCDF4_CLASSIC')
    model = ModelNc(nc_raw, nc, data)
    model.copy_file_contents()
    uuid = model.add_uuid()
    model.add_global_attributes()
    try:
        model.check_time_dimension()
    except ValueError:
        model.nc.close()
        model.nc_raw.close()
        raise MiscError('Incomplete model file.')
    model.add_date()
    model.add_history()
    model.close()
    shutil.copy(temp_file.name, data['full_path'])
    return uuid


def harmonize_hatpro_file(data: dict) -> str:
    """Harmonizes calibrated hatpro netCDF file."""
    temp_file = NamedTemporaryFile()
    nc_raw = netCDF4.Dataset(data['full_path'], 'r')
    nc = netCDF4.Dataset(temp_file.name, 'w', format='NETCDF4_CLASSIC')
    hatpro = HatproNc(nc_raw, nc, data)
    hatpro.copy_file()
    hatpro.add_lwp()
    hatpro.sort_time()
    hatpro.convert_time()
    hatpro.check_time_reference()
    hatpro.add_geolocation()
    hatpro.clean_global_attributes()
    uuid = hatpro.add_uuid()
    hatpro.add_date()
    hatpro.add_global_attributes()
    hatpro.add_history()
    hatpro.close()
    shutil.copy(temp_file.name, data['full_path'])
    return uuid


class Level1Nc:
    def __init__(self, nc_raw: netCDF4.Dataset, nc: netCDF4.Dataset, data: dict):
        self.nc_raw = nc_raw
        self.nc = nc
        self.data = data

    def add_uuid(self) -> str:
        uuid = self.data['uuid'] or get_uuid()
        self.nc.file_uuid = uuid
        return uuid

    def add_history(self):
        old_history = getattr(self.nc_raw, 'history', '')
        history = f"{get_time()} - Metadata harmonized by CLU using data-processing Python package."
        if len(old_history) > 0:
            history = f"{history}\n{old_history}"
        self.nc.history = history

    def close(self):
        self.nc.close()
        self.nc_raw.close()

    def copy_file_contents(self):
        for key, dimension in self.nc_raw.dimensions.items():
            self.nc.createDimension(key, dimension.size)
        for name, variable in self.nc_raw.variables.items():
            dtype = variable.dtype
            var_out = self.nc.createVariable(name,
                                             dtype,
                                             variable.dimensions,
                                             zlib=True,
                                             fill_value=getattr(variable, '_FillValue', None))
            self._copy_variable_attributes(variable, var_out)
            var_out[:] = variable[:]
        self._copy_global_attributes()


    @staticmethod
    def _copy_variable_attributes(source, target):
        attr = {k: source.getncattr(k) for k in source.ncattrs() if k != '_FillValue'}
        target.setncatts(attr)

    def _copy_global_attributes(self):
        for name in self.nc_raw.ncattrs():
            setattr(self.nc, name, self.nc_raw.getncattr(name))


class LegacyNc(Level1Nc):
    pass


class ModelNc(Level1Nc):

    def check_time_dimension(self):
        n_steps = len(self.nc.dimensions['time'])
        n_steps_expected = 25
        n_steps_expected_gdas1 = 9
        if self.data['model'] == 'gdas1' and n_steps == n_steps_expected_gdas1:
            return
        if self.data['model'] != 'gdas1' and n_steps == n_steps_expected:
            return
        raise ValueError

    def add_date(self):
        date_string = self.nc.variables['time'].units
        date = date_string.split()[2]
        self.nc.year, self.nc.month, self.nc.day = date.split('-')

    def add_global_attributes(self):
        self.nc.cloudnet_file_type = 'model'
        self.nc.Conventions = 'CF-1.8'

class HatproNc(Level1Nc):

    bad_lwp_keys = ('LWP', 'LWP_data', 'clwvi', 'atmosphere_liquid_water_content')

    def copy_file(self):
        valid_ind = self._get_valid_timestamps()
        possible_keys = ('lwp', 'time') + self.bad_lwp_keys
        self._copy_file_contents(valid_ind, possible_keys)

    def add_lwp(self):
        key = 'lwp'
        for invalid_name in self.bad_lwp_keys:
            if invalid_name in self.nc.variables:
                self.nc.renameVariable(invalid_name, key)
        assert key in self.nc.variables
        lwp = self.nc.variables[key]
        if 'kg' in lwp.units:
            lwp[:] *= 1000
        lwp.units = COMMON_ATTRIBUTES[key].units
        lwp.long_name = COMMON_ATTRIBUTES[key].long_name

    def sort_time(self):
        time = self.nc.variables['time'][:]
        array = self.nc.variables['lwp'][:]
        ind = time.argsort()
        self.nc.variables['time'][:] = time[ind]
        self.nc.variables['lwp'][:] = array[ind]

    def convert_time(self):
        time = self.nc.variables['time']
        if max(time[:]) > 24:
            fraction_hour = cloudnetpy.utils.seconds2hours(time[:])
            time[:] = fraction_hour
        time.long_name = 'Time UTC'
        time.units = f'hours since {self.data["date"]} 00:00:00 +00:00'
        if hasattr(time, 'comment'):
            delattr(time, 'comment')
        time.standard_name = 'time'
        time.axis = 'T'
        time.calendar = 'standard'

    def check_time_reference(self):
        key = 'time_reference'
        if key in self.nc_raw.variables:
            assert self.nc_raw.variables[key][:] == 1  # 1 = UTC

    def add_geolocation(self):
        for key in ('altitude', 'latitude', 'longitude'):
            var = self.nc.createVariable(key, 'f4')
            var[:] = self.data['site_meta'][key]
            var.units = COMMON_ATTRIBUTES[key].units
            var.long_name = COMMON_ATTRIBUTES[key].long_name
            var.standard_name = COMMON_ATTRIBUTES[key].standard_name

    def clean_global_attributes(self):
        for attr in self.nc.ncattrs():
            delattr(self.nc, attr)

    def add_date(self):
        self.nc.year, self.nc.month, self.nc.day = self.data['date'].split('-')

    def add_global_attributes(self):
        instrument = instruments.HATPRO
        location = utils.read_site_info(self.data['site_name'])['name']
        self.nc.Conventions = 'CF-1.8'
        self.nc.cloudnet_file_type = 'mwr'
        self.nc.source = output.get_l1b_source(instrument)
        self.nc.location = location
        self.nc.title = output.get_l1b_title(instrument, location)
        self.nc.references = output.get_references()

    def _get_valid_timestamps(self) -> list:
        time_stamps = self.nc_raw.variables['time'][:]
        epoch = _get_epoch(self.nc_raw.variables['time'].units)
        expected_date = self.data['date'].split('-')
        valid_ind = []
        for ind, t in enumerate(time_stamps):
            if (0 < t < 24 and epoch == expected_date) or (seconds2date(t, epoch)[:3] == expected_date):
                valid_ind.append(ind)
        if not valid_ind:
            raise RuntimeError('All HATPRO dates differ from expected.')
        return valid_ind

    def _copy_file_contents(self, time_ind: list, keys: tuple):
        for key, dimension in self.nc_raw.dimensions.items():
            size = len(time_ind) if key == 'time' else dimension.size
            self.nc.createDimension(key, size)
        for name, variable in self.nc_raw.variables.items():
            if name not in keys:
                continue
            if name == 'time' and 'int' in str(variable.dtype):
                dtype = 'f8'
            else:
                dtype = variable.dtype
            var_out = self.nc.createVariable(name,
                                             dtype,
                                             variable.dimensions,
                                             zlib=True,
                                             fill_value=getattr(variable, '_FillValue', None))
            self._copy_variable_attributes(variable, var_out)
            var_out[:] = variable[time_ind] if 'time' in variable.dimensions else variable[:]
        self._copy_global_attributes()


def _get_epoch(units: str) -> tuple:
    fallback = (2001, 1, 1)
    try:
        date = units.split()[2]
    except IndexError:
        return fallback
    date = date.replace(',', '')
    try:
        date_components = [int(x) for x in date.split('-')]
    except ValueError:
        try:
            date_components = [int(x) for x in date.split('.')]
        except ValueError:
            return fallback
    year, month, day = date_components
    current_year = datetime.datetime.today().year
    if (1900 < year <= current_year) and (0 < month < 13) and (0 < day < 32):
        return tuple(date_components)
    return fallback
