import datetime
import glob
import gzip
import logging
import os
import pathlib
import shutil

from cloudnetpy.exceptions import DisdrometerDataError
from cloudnetpy.instruments import (
    basta2nc,
    ceilo2nc,
    copernicus2nc,
    galileo2nc,
    hatpro2nc,
    mira2nc,
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

from data_processing import concat_wrapper, nc_header_augmenter, utils
from data_processing.processing_tools import Uuid
from data_processing.utils import RawDataMissingError, SkipBlock, fetch_calibration


class ProcessInstrument:
    def __init__(self, base, temp_file, uuid: Uuid):
        self.base = base
        self.temp_file = temp_file
        self.uuid = uuid
        self.instrument_pids: list = []
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
        full_paths, raw_uuids, self.instrument_pids = self.base.download_instrument(
            "rpg-fmcw-94", ".lv1$"
        )
        self.uuid.product, valid_full_paths = rpg2nc(
            self.base.temp_dir.name, *self._args, **self._kwargs
        )
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)

    def process_mira(self):
        full_paths, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "mira"
        )
        _unzip_gz_files(full_paths)
        self._fix_suffices(self.base.temp_dir.name, ".mmclx")
        self.uuid.product = mira2nc(
            self.base.temp_dir.name, *self._args, **self._kwargs
        )

    def process_basta(self):
        full_path, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "basta", largest_only=True
        )
        self.uuid.product = basta2nc(full_path, *self._args, **self._kwargs)

    def process_copernicus(self):
        full_paths, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "copernicus"
        )
        self.uuid.product = copernicus2nc(
            os.path.dirname(full_paths[0]), *self._args, **self._kwargs
        )

    def process_galileo(self):
        full_paths, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "galileo"
        )
        self.uuid.product = galileo2nc(
            os.path.dirname(full_paths[0]), *self._args, **self._kwargs
        )

    @staticmethod
    def _fix_suffices(dir_name: str, suffix: str) -> None:
        """Fixes filenames that have incorrect suffix."""
        for filename in glob.glob(f"{dir_name}/*"):
            if not filename.lower().endswith((".gz", suffix)):
                os.rename(filename, filename + suffix)


class ProcessDopplerLidar(ProcessInstrument):
    def process_halo_doppler_lidar(self):
        full_paths, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "halo-doppler-lidar",
            include_pattern=r"Stare.*\.hpl",
            exclude_tag_subset={"cross"},
        )
        full_paths_bg, _, _ = self.base.download_instrument(
            "halo-doppler-lidar",
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


class ProcessLidar(ProcessInstrument):
    file_id = "clu-generated-daily"

    def process_chm15k(self):
        self._process_chm_lidar("chm15k")

    def process_chm15x(self):
        self._process_chm_lidar("chm15x")

    def process_chm15kx(self):
        self._process_chm_lidar("chm15kx")

    def _process_chm_lidar(self, model: str):
        full_paths, raw_uuids, self.instrument_pids = self.base.download_instrument(
            model
        )
        valid_full_paths = concat_wrapper.concat_chm15k_files(
            full_paths, self.base.date_str, self.base.daily_file.name
        )
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)
        utils.check_chm_version(self.base.daily_file.name, model)
        self._call_ceilo2nc()

    def process_ct25k(self):
        full_path, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "ct25k", largest_only=True
        )
        shutil.move(full_path, self.base.daily_file.name)
        self._call_ceilo2nc()

    def process_halo_doppler_lidar_calibrated(self):
        self._process_halo_lidar_calibrated()

    def _process_halo_lidar_calibrated(self):
        full_path, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "halo-doppler-lidar-calibrated", largest_only=True
        )
        data = self._get_payload_for_nc_file_augmenter(full_path)
        self.uuid.product = nc_header_augmenter.harmonize_halo_calibrated_file(data)

    def process_pollyxt(self):
        full_paths, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "pollyxt"
        )
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
        full_paths, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "cl31"
        )
        full_paths.sort()
        utils.concatenate_text_files(full_paths, self.base.daily_file.name)
        self._call_ceilo2nc()

    def process_cl51(self):
        if self.base.site == "norunda":
            (
                full_paths,
                self.uuid.raw,
                self.instrument_pids,
            ) = self.base.download_adjoining_daily_files("cl51")
        else:
            (
                full_paths,
                self.uuid.raw,
                self.instrument_pids,
            ) = self.base.download_instrument("cl51")
        full_paths.sort()
        utils.concatenate_text_files(full_paths, self.base.daily_file.name)
        date = utils.date_string_to_date(self.base.date_str)
        if self.base.site == "norunda" and date < datetime.date(2021, 10, 18):
            logging.info("Shifting timestamps to UTC")
            offset_in_hours = -1
            _fix_cl51_timestamps(self.base.daily_file.name, offset_in_hours)
        self._call_ceilo2nc()

    def process_cl61d(self):
        model = "cl61d"
        try:
            if self.base.is_reprocess:
                raise SkipBlock  # Move to next block and re-create daily file
            tmp_file, _, self.instrument_pids = self.base.download_instrument(
                model, include_pattern=self.file_id, largest_only=True
            )
            full_paths, raw_uuids, self.instrument_pids = self.base.download_uploaded(
                model, exclude_pattern=self.file_id
            )
            valid_full_paths = concat_wrapper.update_daily_file(full_paths, tmp_file)
            shutil.copy(tmp_file, self.base.daily_file.name)
            msg = "Raw data already processed"
        except (RawDataMissingError, SkipBlock) as exc:
            msg = ""
            full_paths, raw_uuids, self.instrument_pids = self.base.download_instrument(
                model, exclude_pattern=self.file_id
            )
            if full_paths:
                logging.info(f"Creating daily file from {len(full_paths)} files")
            else:
                raise RawDataMissingError from exc
            variables = ["x_pol", "p_pol", "beta_att", "time"]
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
            raise RawDataMissingError(msg)
        filename = self._create_daily_file_name(model)
        try:
            instrument_pid = self.instrument_pids[0]
        except IndexError:
            instrument_pid = None
        self.base.md_api.upload_instrument_file(
            self.base, model, filename, instrument_pid
        )
        self._call_ceilo2nc()
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)

    def _create_daily_file_name(self, model: str) -> str:
        date = self.base.date_str.replace("-", "")
        return f"{date}_{self.base.site}_{model}_{self.file_id}.nc"

    def _call_ceilo2nc(self):
        calibration = self._fetch_ceilo_calibration()
        site_meta = self.base.site_meta | calibration
        self.uuid.product = ceilo2nc(
            self.base.daily_file.name,
            self.temp_file.name,
            site_meta=site_meta,
            uuid=self.uuid.volatile,
            date=self.base.date_str,
        )

    def _fetch_ceilo_calibration(self) -> dict:
        output: dict = {}
        if not self.instrument_pids:
            return output
        instrument_pid = self.instrument_pids[0]
        calibration = fetch_calibration(instrument_pid, self.base.date_str)
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
        if not self.instrument_pids:
            return output
        instrument_pid = self.instrument_pids[0]
        calibration = fetch_calibration(instrument_pid, self.base.date_str)
        if not calibration:
            return output
        if "snr_limit" in calibration["data"]:
            output["snr_limit"] = float(calibration["data"]["snr_limit"])
        return output


class ProcessMwr(ProcessInstrument):
    def process_hatpro(self):
        try:
            full_paths, raw_uuids, self.instrument_pids = self.base.download_instrument(
                "hatpro", r"^(?!.*scan).*\.lwp$|^(?!.*scan).*\.iwv$"
            )
            self.uuid.product, valid_full_paths = hatpro2nc(
                self.base.temp_dir.name, *self._args, **self._kwargs
            )
        except RawDataMissingError:
            pattern = "(ufs_l2a.nc$|clwvi.*.nc$|.lwp.*.nc$)"
            full_paths, raw_uuids, self.instrument_pids = self.base.download_instrument(
                "hatpro", pattern
            )
            valid_full_paths = concat_wrapper.concat_netcdf_files(
                full_paths, self.base.date_str, self.temp_file.name
            )
            data = self._get_payload_for_nc_file_augmenter(self.temp_file.name)
            self.uuid.product = nc_header_augmenter.harmonize_hatpro_file(data)
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)

    def process_radiometrics(self):
        full_paths, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "radiometrics"
        )
        _unzip_gz_files(full_paths)
        self.uuid.product = radiometrics2nc(
            self.base.temp_dir.name, *self._args, **self._kwargs
        )


class ProcessDisdrometer(ProcessInstrument):
    def process_parsivel(self):
        full_path, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "parsivel", largest_only=True
        )
        telegram = None
        if self.base.site in ["norunda", "ny-alesund", "juelich"]:
            telegram = [
                1,
                2,
                3,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                16,
                17,
                18,
                22,
                24,
                25,
                90,
                91,
                93,
            ]
        if full_path.endswith(".nc"):
            data = self._get_payload_for_nc_file_augmenter(self.temp_file.name)
            try:
                self.uuid.product = nc_header_augmenter.harmonize_parsivel_file(data)
            except OSError:
                raise DisdrometerDataError("Unable to process")
        else:
            kwargs = self._kwargs.copy()
            kwargs["telegram"] = telegram
            self.uuid.product = parsivel2nc(full_path, *self._args, **kwargs)

    def process_thies_lnm(self):
        full_paths, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "thies-lnm"
        )
        full_paths.sort()
        utils.concatenate_text_files(full_paths, self.base.daily_file.name)
        self.uuid.product = thies2nc(
            self.base.daily_file.name, *self._args, **self._kwargs
        )


class ProcessWeatherStation(ProcessInstrument):
    def process_weather_station(self):
        full_path, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "weather-station", largest_only=True
        )
        self.uuid.product = ws2nc(full_path, *self._args, **self._kwargs)


def _get_valid_uuids(uuids: list, full_paths: list, valid_full_paths: list) -> list:
    return [
        uuid
        for uuid, full_path in zip(uuids, full_paths)
        if full_path in valid_full_paths
    ]


def _unzip_gz_files(full_paths: list):
    for path_in in full_paths:
        if not path_in.endswith(".gz"):
            continue
        logging.info(f"Decompressing {path_in}")
        path_out = path_in.removesuffix(".gz")
        with gzip.open(path_in, "rb") as file_in:
            with open(path_out, "wb") as file_out:
                shutil.copyfileobj(file_in, file_out)
        os.remove(path_in)


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
