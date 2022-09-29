import os
import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile

import netCDF4
import numpy as np
import pytest
from cloudnetpy.exceptions import ValidTimeStampError
from cloudnetpy.metadata import COMMON_ATTRIBUTES
from cloudnetpy_qc import quality

from data_processing import nc_header_augmenter as nca
from data_processing.utils import MiscError

TEST_FILE_PATH = Path(__file__).parent.absolute()


class TestMwr:
    @pytest.fixture(autouse=True)
    def _before_and_after(self):
        self.data = {
            "site_name": "bucharest",
            "date": "2020-10-23",
            "uuid": None,
            "site_meta": {"altitude": 10, "latitude": 25, "longitude": 52.5},
        }
        yield

    def test_standard_fix(self, mwr_file):
        self.data["full_path"] = mwr_file
        self.data["original_filename"] = os.path.basename(mwr_file)
        uuid = nca.harmonize_hatpro_file(self.data)
        nc = netCDF4.Dataset(mwr_file)
        assert len(nc.variables["time"][:]) == 5
        assert len(uuid) == 36
        assert nc.file_uuid == uuid
        assert nc.year == "2020"
        assert nc.month == "10"
        assert nc.day == "23"
        assert nc.location == "Bucharest"
        assert nc.title == "HATPRO microwave radiometer from Bucharest"
        assert nc.Conventions == "CF-1.8"
        nc.close()
        run_quality_tests(self.data["full_path"])

    def test_user_supplied_uuid(self, mwr_file):
        uuid_volatile = "abc"
        self.data["full_path"] = mwr_file
        self.data["original_filename"] = os.path.basename(mwr_file)
        self.data["uuid"] = uuid_volatile
        uuid = nca.harmonize_hatpro_file(self.data)
        nc = netCDF4.Dataset(mwr_file)
        assert uuid == uuid_volatile
        assert nc.file_uuid == uuid_volatile
        nc.close()

    def test_null_latitude(self, mwr_file):
        self.data["full_path"] = mwr_file
        self.data["original_filename"] = os.path.basename(mwr_file)
        self.data["latitude"] = None
        uuid = nca.harmonize_hatpro_file(self.data)
        with netCDF4.Dataset(mwr_file) as nc:
            assert nc.variables["latitude"].shape == ()

    def test_wrong_date(self, mwr_file):
        self.data["full_path"] = mwr_file
        self.data["original_filename"] = os.path.basename(mwr_file)
        self.data["date"] = "2020-10-24"
        with pytest.raises(ValidTimeStampError):
            nca.harmonize_hatpro_file(self.data)

    def test_hatpro_harmonization(self, mwr_file):
        self.data["full_path"] = mwr_file
        self.data["original_filename"] = os.path.basename(mwr_file)
        nca.harmonize_hatpro_file(self.data)
        nc = netCDF4.Dataset(mwr_file)
        key = "lwp"
        for attr in ("long_name", "units", "standard_name"):
            assert getattr(nc.variables[key], attr) == getattr(COMMON_ATTRIBUTES[key], attr)
        time = nc.variables["time"][:]
        assert nc.variables["time"].units == f'hours since {self.data["date"]} 00:00:00 +00:00'
        assert nc.variables["time"].dtype == "double"
        assert np.all(np.diff(time) > 0)
        for key in ("altitude", "latitude", "longitude"):
            assert key in nc.variables
            assert nc.variables[key][:] == float(self.data["site_meta"][key]), key
            for attr in ("long_name", "units"):
                assert getattr(nc.variables[key], attr) == getattr(COMMON_ATTRIBUTES[key], attr)
        nc.close()
        run_quality_tests(self.data["full_path"])

    def test_palaiseau_mwr(self):
        test_file = f"{TEST_FILE_PATH}/../data/raw/hatpro/palaiseau_hatpro.nc"
        temp_file = NamedTemporaryFile(suffix=".nc")
        shutil.copy(test_file, temp_file.name)
        self.data["full_path"] = temp_file.name
        self.data["date"] = "2021-10-07"
        nca.harmonize_hatpro_file(self.data)
        nc = netCDF4.Dataset(self.data["full_path"])
        assert nc.Conventions == "CF-1.8"
        assert nc.variables["lwp"].units == "g m-2"
        time = nc.variables["time"][:]
        assert nc.variables["time"].units == f'hours since {self.data["date"]} 00:00:00 +00:00'
        assert nc.variables["time"].dtype == "double"
        assert np.all(np.diff(time) > 0)
        nc.close()
        run_quality_tests(self.data["full_path"])


@pytest.mark.parametrize(
    "filename, site",
    [
        ("220201_ufs_l2a.nc", "schneefernerhaus"),
        ("sups_nya_mwr00_l2_clwvi_v00_20220201000035.nc", "ny-alesund"),
        ("sups_joy_mwr00_l2_clwvi_p00_20220201000000.nc", "juelich"),
        ("hatpro_0a_Lz1Lb87Imwrad-LWP_v01_20220201_000240.nc", "palaiseau"),
    ],
)
def test_production_mwr_files(filename, site):
    test_file = f"{TEST_FILE_PATH}/../data/raw/hatpro/{filename}"
    temp_file = NamedTemporaryFile(suffix=".nc")
    shutil.copy(test_file, temp_file.name)
    data = {
        "site_name": site,
        "date": "2022-02-01",
        "uuid": None,
        "site_meta": {"altitude": 10, "latitude": 25, "longitude": 52.5},
        "full_path": temp_file.name,
    }
    nca.harmonize_hatpro_file(data)
    run_quality_tests(data["full_path"])


@pytest.mark.parametrize(
    "filename",
    [
        "halo-raw.nc",
        "halo-bad-times.nc",
    ],
)
def test_halo_fix(filename):
    test_file = f"{TEST_FILE_PATH}/../data/raw/halo/{filename}"
    temp_file = NamedTemporaryFile(suffix=".nc")
    shutil.copy(test_file, temp_file.name)
    data = {
        "site_name": "hyytiala",
        "date": "2020-01-07",
        "uuid": None,
        "site_meta": {"altitude": 10, "latitude": 25, "longitude": 52.5},
        "full_path": temp_file.name,
    }
    nca.harmonize_halo_file(data)
    run_quality_tests(data["full_path"])
    nc = netCDF4.Dataset(data["full_path"])
    wavelength = nc.variables["wavelength"][:]
    assert (
        1490 < wavelength < 1560
    )  # It should be 1550, but it is incorrectly 1.5e-6 um in the daily file.
    nc.close()


class TestModel:
    @pytest.fixture(autouse=True)
    def _before_and_after(self):
        self.data = {
            "site_name": "punta-arenas",
            "date": "2021-11-20",
            "uuid": None,
            "instrument": None,
            "model": "ecmwf",
            "original_filename": "model-file.nc",
        }

    def test_model_fix(self, model_file):
        self.data["full_path"] = model_file
        uuid = nca.harmonize_model_file(self.data)
        nc = netCDF4.Dataset(model_file)
        assert len(uuid) == 36
        assert nc.file_uuid == uuid
        assert nc.year == "2020"
        assert nc.month == "10"
        assert nc.day == "14"
        assert nc.title == "ECMWF single-site output over Bucharest"
        assert nc.Conventions == "CF-1.8"
        assert nc.variables["time"].dtype == "float32"
        nc.close()

    def test_bad_model_file(self, bad_gdas1_file):
        self.data["full_path"] = bad_gdas1_file
        self.data["model"] = "gdas1"
        with pytest.raises(MiscError):
            nca.harmonize_model_file(self.data)

    def test_punta_arenas_ecmwf(self):
        test_file = f"{TEST_FILE_PATH}/../data/raw/model/20211120_punta-arenas_ecmwf.nc"
        temp_file = NamedTemporaryFile(suffix=".nc")
        shutil.copy(test_file, temp_file.name)
        self.data["full_path"] = temp_file.name
        self.data["date"] = "2021-11-20"
        self.data["site"] = "punta-arenas"
        nca.harmonize_model_file(self.data)
        nc = netCDF4.Dataset(self.data["full_path"])
        assert nc.Conventions == "CF-1.8"
        time = nc.variables["time"][:]
        assert nc.variables["time"].units == f'hours since {self.data["date"]} 00:00:00 +00:00'
        assert nc.variables["time"].dtype == "float32"
        assert np.all(np.diff(time) > 0)
        nc.close()
        run_quality_tests(self.data["full_path"])

    def test_bucharest_ecmwf(self):
        test_file = f"{TEST_FILE_PATH}/../data/raw/model/20220118_bucharest_ecmwf.nc"
        temp_file = NamedTemporaryFile(suffix=".nc")
        shutil.copy(test_file, temp_file.name)
        self.data["full_path"] = temp_file.name
        self.data["date"] = "2022-01-18"
        self.data["site"] = "bucharest"
        nca.harmonize_model_file(self.data)
        run_quality_tests(self.data["full_path"])


@pytest.mark.parametrize(
    "arg, expected",
    [
        ("seconds from 2020-01-15 00:00:00", (2020, 1, 15)),
        ("seconds from 2020.1.15", (2020, 1, 15)),
        ("seconds from bad-times 00:00:00", (2001, 1, 1)),
        ("blabla", (2001, 1, 1)),
    ],
)
def test_get_epoch(arg, expected):
    assert nca._get_epoch(arg) == expected


def run_quality_tests(filename: str):
    quality.run_tests(Path(filename))
