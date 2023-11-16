import datetime
import gzip
import logging
import os
import pathlib
import shutil
import tempfile

from cloudnetpy.instruments import (
    basta2nc,
    ceilo2nc,
    copernicus2nc,
    galileo2nc,
    hatpro2l1c,
    hatpro2nc,
    mira2nc,
    mrr2nc,
    parsivel2nc,
    pollyxt2nc,
    radiometrics2nc,
    rpg2nc,
    thies2nc,
    ws2nc,
)
from cloudnetpy.utils import is_timestamp
from haloreader.read import read as read_halo
from haloreader.read import read_bg as read_halobg
from requests.exceptions import HTTPError

from data_processing import concat_wrapper, nc_header_augmenter, utils
from data_processing.processing_tools import Uuid
from data_processing.utils import MiscError, RawDataMissingError, fetch_calibration


class ProcessInstrument:
    def __init__(self, base, temp_file, uuid: Uuid, instrument_pid: str):
        self.base = base
        self.temp_file = temp_file
        self.uuid = uuid
        self.instrument_pid = instrument_pid
        self._kwargs = self._get_kwargs()
        self._args = self._get_args()

    def _get_args(self) -> tuple:
        return self.temp_file.name, self.base.site_meta

    def _get_kwargs(self) -> dict:
        return {"uuid": self.uuid.volatile, "date": self.base.date_str}

    def _get_payload_for_nc_file_augmenter(self, full_path: str) -> dict:
        return {
            "site_name": self.base.site,
            "date": self.base.date_str,
            "site_meta": self.base.site_meta,
            "full_path": full_path,
            "uuid": self.uuid.volatile,
        }


class ProcessRadar(ProcessInstrument):
    def process_rpg_fmcw_94(self):
        full_paths, raw_uuids = self.base.download_instrument(
            self.instrument_pid, include_pattern=r"zen.*\.lv1$"
        )
        self.uuid.product, valid_full_paths = rpg2nc(
            self.base.temp_dir.name, *self._args, **self._kwargs
        )
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)

    def process_mira(self):
        full_paths, self.uuid.raw = self.base.download_instrument(self.instrument_pid)
        full_paths = _unzip_gz_files(full_paths)
        full_paths = self._fix_suffices(full_paths, ".mmclx")
        self.uuid.product = mira2nc(full_paths, *self._args, **self._kwargs)

    def process_basta(self):
        full_path, self.uuid.raw = self.base.download_instrument(
            self.instrument_pid, largest_only=True
        )
        self.uuid.product = basta2nc(full_path, *self._args, **self._kwargs)

    def process_copernicus(self):
        full_paths, self.uuid.raw = self.base.download_instrument(self.instrument_pid)
        self._add_calibration("range_offset", 0)
        self.uuid.product = copernicus2nc(
            os.path.dirname(full_paths[0]), *self._args, **self._kwargs
        )

    def process_galileo(self):
        full_paths, self.uuid.raw = self.base.download_instrument(self.instrument_pid)
        self.uuid.product = galileo2nc(
            os.path.dirname(full_paths[0]), *self._args, **self._kwargs
        )

    @staticmethod
    def _fix_suffices(full_paths: list[str], suffix: str) -> list[str]:
        """Fixes filenames that have incorrect suffix."""
        out_paths = []
        for filename in full_paths:
            if not filename.lower().endswith((".gz", suffix)):
                new_filename = filename + suffix
                os.rename(filename, new_filename)
                out_paths.append(new_filename)
            else:
                out_paths.append(filename)
        return out_paths

    def _add_calibration(
        self, key: str, default_value: float, api_key: str | None = None
    ) -> None:
        calibration = fetch_calibration(self.instrument_pid, self.base.date_str)
        if calibration is not None:
            data = calibration["data"]
            self.base.site_meta[key] = data.get(api_key or key, default_value)


class ProcessDopplerLidar(ProcessInstrument):
    def process_halo_doppler_lidar(self):
        full_paths, self.uuid.raw = self.base.download_instrument(
            self.instrument_pid,
            include_pattern=r"Stare.*\.hpl",
            exclude_tag_subset={"cross"},
        )
        full_paths_bg, _ = self.base.download_instrument(
            self.instrument_pid,
            include_pattern=r"Background.*\.txt",
            exclude_tag_subset={"cross"},
            date_from=str(
                datetime.date.fromisoformat(self.base.date_str)
                - datetime.timedelta(days=30)
            ),
        )
        full_paths_ = [pathlib.Path(path) for path in full_paths]
        full_paths_bg_ = [pathlib.Path(path) for path in full_paths_bg]
        halo = read_halo(full_paths_)
        halobg = read_halobg(full_paths_bg_)
        if halo is None:
            raise RawDataMissingError
        if halobg is None:
            raise RawDataMissingError
        halo.correct_background(halobg)
        halo.compute_beta()
        screen = halo.compute_noise_screen()
        halo.compute_beta_screened(screen)
        halo.compute_doppler_velocity_screened(screen)
        halo.convert_time_unit2cloudnet_time()
        nc_buf = halo.to_nc(
            nc_map={
                "variables": {
                    "beta": "beta_raw",
                    "beta_screened": "beta",
                    "doppler_velocity_screened": "v",
                }
            },
            nc_exclude={"variables": {"beta_raw"}},
        )
        self.temp_file.write(nc_buf)
        data = self._get_payload_for_nc_file_augmenter(self.temp_file.name)
        self.uuid.product = nc_header_augmenter.harmonize_halo_file(data)

    def process_wls100s(self):
        raise NotImplementedError()

    def process_wls200s(self):
        raise NotImplementedError()

    def process_wls400s(self):
        raise NotImplementedError()


class ProcessLidar(ProcessInstrument):
    def process_cs135(self):
        full_paths, self.uuid.raw = self.base.download_instrument(self.instrument_pid)
        full_paths.sort()
        utils.concatenate_text_files(full_paths, self.base.daily_file.name)
        self._call_ceilo2nc("cs135")

    def process_chm15k(self):
        self._process_chm_lidar("chm15k")

    def process_chm15x(self):
        self._process_chm_lidar("chm15x")

    def process_chm15kx(self):
        self._process_chm_lidar("chm15kx")

    def _process_chm_lidar(self, model: str):
        full_paths, raw_uuids = self.base.download_instrument(self.instrument_pid)
        valid_full_paths = concat_wrapper.concat_chm15k_files(
            full_paths, self.base.date_str, self.base.daily_file.name
        )
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)
        utils.check_chm_version(self.base.daily_file.name, model)
        self._call_ceilo2nc("chm15k")

    def process_ct25k(self):
        full_paths, self.uuid.raw = self.base.download_instrument(self.instrument_pid)
        full_paths.sort()
        full_paths = _unzip_gz_files(full_paths)
        utils.concatenate_text_files(full_paths, self.base.daily_file.name)
        self._call_ceilo2nc("ct25k")

    def process_halo_doppler_lidar_calibrated(self):
        full_path, self.uuid.raw = self.base.download_instrument(
            self.instrument_pid, largest_only=True
        )
        data = self._get_payload_for_nc_file_augmenter(full_path)
        self.uuid.product = nc_header_augmenter.harmonize_halo_calibrated_file(data)

    def process_pollyxt(self):
        full_paths, self.uuid.raw = self.base.download_instrument(self.instrument_pid)
        calibration = self._fetch_pollyxt_calibration()
        site_meta = self.base.site_meta | calibration
        self.uuid.product = pollyxt2nc(
            os.path.dirname(full_paths[0]),
            self.temp_file.name,
            site_meta=site_meta,
            uuid=self.uuid.volatile,
            date=self.base.date_str,
        )

    def process_cl31(self):
        full_paths, self.uuid.raw = self.base.download_instrument(self.instrument_pid)
        full_paths.sort()
        utils.concatenate_text_files(full_paths, self.base.daily_file.name)
        self._call_ceilo2nc("cl31")

    def process_cl51(self):
        if self.base.site == "norunda":
            (
                full_paths,
                self.uuid.raw,
            ) = self.base.download_adjoining_daily_files(self.instrument_pid)
        else:
            (
                full_paths,
                self.uuid.raw,
            ) = self.base.download_instrument(self.instrument_pid)
        full_paths.sort()
        utils.concatenate_text_files(full_paths, self.base.daily_file.name)
        date = utils.date_string_to_date(self.base.date_str)
        if self.base.site == "norunda" and date < datetime.date(2021, 10, 18):
            logging.info("Shifting timestamps to UTC")
            offset_in_hours = -1
            _fix_cl51_timestamps(self.base.daily_file.name, offset_in_hours)
        self._call_ceilo2nc("cl51")

    def process_cl61d(self):
        full_paths, raw_uuids = self.base.download_instrument(
            self.instrument_pid, exclude_pattern="clu-generated"
        )
        variables = ["x_pol", "p_pol", "beta_att", "time", "tilt_angle"]
        try:
            valid_full_paths = concat_wrapper.concat_netcdf_files(
                full_paths,
                self.base.date_str,
                self.base.daily_file.name,
                variables=variables,
            )
        except KeyError:
            valid_full_paths = concat_wrapper.concat_netcdf_files(
                full_paths,
                self.base.date_str,
                self.base.daily_file.name,
                concat_dimension="profile",
                variables=variables,
            )
        if not valid_full_paths:
            raise RawDataMissingError()
        self._call_ceilo2nc("cl61d")
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)

    def _call_ceilo2nc(self, model: str):
        calibration = self._fetch_ceilo_calibration()
        site_meta = self.base.site_meta | calibration
        site_meta["model"] = model
        self.uuid.product = ceilo2nc(
            self.base.daily_file.name,
            self.temp_file.name,
            site_meta=site_meta,
            uuid=self.uuid.volatile,
            date=self.base.date_str,
        )

    def _fetch_ceilo_calibration(self) -> dict:
        output: dict = {}
        calibration = fetch_calibration(self.instrument_pid, self.base.date_str)
        if not calibration:
            return output
        if "calibration_factor" in calibration["data"]:
            output["calibration_factor"] = float(
                calibration["data"]["calibration_factor"]
            )
        if "range_corrected" in calibration["data"]:
            output["range_corrected"] = calibration["data"]["range_corrected"]
        return output

    def _fetch_pollyxt_calibration(self) -> dict:
        output = {"snr_limit": 25.0}
        calibration = fetch_calibration(self.instrument_pid, self.base.date_str)
        if not calibration:
            return output
        if "snr_limit" in calibration["data"]:
            output["snr_limit"] = float(calibration["data"]["snr_limit"])
        return output


class ProcessMwrL1c(ProcessInstrument):
    def process_hatpro(self):
        data = self._get_calibration_data()
        full_paths, self.uuid.raw = self.base.download_instrument(
            self.instrument_pid,
            include_pattern=r"\.(brt|hkd|met|irt|blb|bls)$",
        )
        output_filename, site_meta = self._args

        with tempfile.TemporaryDirectory() as temp_dir:
            site_meta = {**site_meta, **data["data"]}
            coeff_paths = []
            for link in data["data"]["coefficientLinks"]:
                filename = link.split("/")[-1]
                full_path = os.path.join(temp_dir, filename)
                with open(full_path, "wb") as f:
                    res = self.base.md_api.session.get(link)
                    res.raise_for_status()
                    f.write(res.content)
                coeff_paths.append(full_path)
            site_meta["coefficientFiles"] = coeff_paths

            self.uuid.product = hatpro2l1c(
                self.base.temp_dir.name, output_filename, site_meta, **self._kwargs
            )

    def _get_calibration_data(self) -> dict:
        payload = {
            "date": self.base.date_str,
            "instrumentPid": self.instrument_pid,
        }
        try:
            data = self.base.md_api.get("api/calibration", payload)
        except HTTPError:
            raise MiscError("Skipping due to missing mwrpy coefficients")
        return data


class ProcessMwr(ProcessInstrument):
    def process_hatpro(self):
        try:
            full_paths, raw_uuids = self.base.download_instrument(
                self.instrument_pid,
                include_pattern=r"^(?!.*scan).*\.lwp$|^(?!.*scan).*\.iwv$",
            )
            self.uuid.product, valid_full_paths = hatpro2nc(
                self.base.temp_dir.name, *self._args, **self._kwargs
            )

        except RawDataMissingError:
            pattern = "(ufs_l2a.nc$|clwvi.*.nc$|.lwp.*.nc$)"
            full_paths, raw_uuids = self.base.download_instrument(
                self.instrument_pid, include_pattern=pattern
            )
            valid_full_paths = concat_wrapper.concat_netcdf_files(
                full_paths, self.base.date_str, self.temp_file.name
            )
            data = self._get_payload_for_nc_file_augmenter(self.temp_file.name)
            self.uuid.product = nc_header_augmenter.harmonize_hatpro_file(data)
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)

    def process_radiometrics(self):
        full_paths, self.uuid.raw = self.base.download_instrument(self.instrument_pid)
        _unzip_gz_files(full_paths)
        self.uuid.product = radiometrics2nc(
            self.base.temp_dir.name, *self._args, **self._kwargs
        )


class ProcessDisdrometer(ProcessInstrument):
    def process_parsivel(self):
        try:
            full_paths, self.uuid.raw = self.base.download_instrument(
                self.instrument_pid, largest_only=True
            )
            data = self._get_payload_for_nc_file_augmenter(self.temp_file.name)
            self.uuid.product = nc_header_augmenter.harmonize_parsivel_file(data)
        except OSError:
            full_paths, self.uuid.raw = self.base.download_instrument(
                self.instrument_pid
            )
            calibration = self._fetch_parsivel_calibration()
            kwargs = self._kwargs.copy()
            kwargs["telegram"] = calibration["telegram"]
            if calibration["missing_timestamps"] is True:
                full_paths, timestamps = utils.deduce_parsivel_timestamps(full_paths)
                kwargs["timestamps"] = timestamps
            self.uuid.product = parsivel2nc(full_paths, *self._args, **kwargs)

    def process_thies_lnm(self):
        full_paths, self.uuid.raw = self.base.download_instrument(self.instrument_pid)
        full_paths.sort()
        utils.concatenate_text_files(full_paths, self.base.daily_file.name)
        self.uuid.product = thies2nc(
            self.base.daily_file.name, *self._args, **self._kwargs
        )

    def _fetch_parsivel_calibration(self) -> dict:
        output: dict = {"telegram": None, "missing_timestamps": False}
        calibration = fetch_calibration(self.instrument_pid, self.base.date_str)
        if calibration is not None:
            data = calibration["data"]
            output["telegram"] = data.get("telegram", None)
            output["missing_timestamps"] = data.get("missing_timestamps", False)
        return output


class ProcessWeatherStation(ProcessInstrument):
    def process_weather_station(self):
        if self.base.site != "palaiseau":
            raise NotImplementedError(
                "Weather station only implemented for Palaiseau data format"
            )
        full_path, self.uuid.raw = self.base.download_instrument(
            self.instrument_pid, largest_only=True
        )
        self.uuid.product = ws2nc(full_path, *self._args, **self._kwargs)


class ProcessRainRadar(ProcessInstrument):
    def process_mrr_pro(self):
        full_paths, self.uuid.raw = self.base.download_instrument(self.instrument_pid)
        self.uuid.product = mrr2nc(full_paths, *self._args, **self._kwargs)


def _get_valid_uuids(uuids: list, full_paths: list, valid_full_paths: list) -> list:
    return [
        uuid
        for uuid, full_path in zip(uuids, full_paths)
        if full_path in valid_full_paths
    ]


def _unzip_gz_file(path_in: str) -> str:
    if not path_in.endswith(".gz"):
        return path_in
    path_out = path_in.removesuffix(".gz")
    logging.debug(f"Decompressing {path_in} to {path_out}")
    with gzip.open(path_in, "rb") as file_in:
        with open(path_out, "wb") as file_out:
            shutil.copyfileobj(file_in, file_out)
    os.remove(path_in)
    return path_out


def _unzip_gz_files(full_paths: list):
    paths_out = []
    for path_in in full_paths:
        paths_out.append(_unzip_gz_file(path_in))
    return paths_out


def _fix_cl51_timestamps(filename: str, hours: int) -> None:
    with open(filename, "r") as file:
        lines = file.readlines()
    for ind, line in enumerate(lines):
        if is_timestamp(line):
            date_time = line.strip("-").strip("\n")
            date_time_utc = utils.shift_datetime(date_time, hours)
            lines[ind] = f"-{date_time_utc}\n"
    with open(filename, "w") as file:
        file.writelines(lines)
