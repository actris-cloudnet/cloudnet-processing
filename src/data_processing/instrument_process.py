import datetime
import glob
import gzip
import logging
import os
import shutil

from cloudnetpy.exceptions import DisdrometerDataError
from cloudnetpy.instruments import (
    basta2nc,
    ceilo2nc,
    copernicus2nc,
    disdrometer2nc,
    hatpro2nc,
    mira2nc,
    pollyxt2nc,
    radiometrics2nc,
    rpg2nc,
    ws2nc,
)
from cloudnetpy.utils import is_timestamp

from data_processing import concat_wrapper, nc_header_augmenter, utils
from data_processing.processing_tools import Uuid
from data_processing.utils import RawDataMissingError, SkipBlock


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

    def _fetch_calibration_factor(self, instrument: str) -> dict:
        meta = self.base.site_meta.copy()
        meta["calibration_factor"] = utils.get_calibration_factor(
            self.base.site, self.base.date_str, instrument
        )
        return meta

    def _fetch_range_corrected(self, site_meta_in: dict) -> dict:
        meta = site_meta_in.copy()
        date = utils.date_string_to_date(self.base.date_str)
        if self.base.site == "norunda" and date > datetime.date(2020, 9, 6):
            meta["range_corrected"] = False
        return meta

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
        dir_name = _unzip_gz_files(full_paths)
        self._fix_suffices(dir_name, ".mmclx")
        self.uuid.product = mira2nc(dir_name, *self._args, **self._kwargs)

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
        raise NotImplementedError("Galileo cloud radar not yet implemented!")

    @staticmethod
    def _fix_suffices(dir_name: str, suffix: str) -> None:
        """Fixes filenames that have incorrect suffix."""
        for filename in glob.glob(f"{dir_name}/*"):
            if not filename.lower().endswith((".gz", suffix)):
                os.rename(filename, filename + suffix)


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
        self._call_ceilo2nc(model)

    def process_ct25k(self):
        full_path, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "ct25k", largest_only=True
        )
        shutil.move(full_path, self.base.daily_file.name)
        self._call_ceilo2nc("ct25k")

    def process_halo_doppler_lidar_calibrated(self):
        self._process_halo_lidar("-calibrated")

    def process_halo_doppler_lidar(self):
        """This can be removed at some point."""
        self._process_halo_lidar()

    def _process_halo_lidar(self, suffix: str = ""):
        full_path, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            f"halo-doppler-lidar{suffix}", largest_only=True
        )
        data = self._get_payload_for_nc_file_augmenter(full_path)
        self.uuid.product = nc_header_augmenter.harmonize_halo_file(data)

    def process_pollyxt(self):
        full_paths, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "pollyxt"
        )
        site_meta = self.base.site_meta
        site_meta["snr_limit"] = 25
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
        self._call_ceilo2nc("cl31")

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
        self._call_ceilo2nc("cl51")

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
        self._call_ceilo2nc(model)
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)

    def _create_daily_file_name(self, model: str) -> str:
        date = self.base.date_str.replace("-", "")
        return f"{date}_{self.base.site}_{model}_{self.file_id}.nc"

    def _call_ceilo2nc(self, instrument: str):
        site_meta = self._fetch_calibration_factor(instrument)
        site_meta = self._fetch_range_corrected(site_meta)
        self.uuid.product = ceilo2nc(
            self.base.daily_file.name,
            self.temp_file.name,
            site_meta=site_meta,
            uuid=self.uuid.volatile,
            date=self.base.date_str,
        )


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
        full_paths.sort()
        for full_path in full_paths[1:]:
            utils.remove_header_lines(full_path, 1)
        utils.concatenate_text_files(full_paths, self.base.daily_file.name)
        self.uuid.product = radiometrics2nc(
            self.base.daily_file.name, *self._args, **self._kwargs
        )


class ProcessDisdrometer(ProcessInstrument):
    def process_parsivel(self):
        full_path, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "parsivel", largest_only=True
        )
        try:
            self.uuid.product = disdrometer2nc(full_path, *self._args, **self._kwargs)
        except DisdrometerDataError:
            data = self._get_payload_for_nc_file_augmenter(self.temp_file.name)
            try:
                self.uuid.product = nc_header_augmenter.harmonize_parsivel_file(data)
            except OSError:
                raise DisdrometerDataError("Unable to process")

    def process_thies_lnm(self):
        full_paths, self.uuid.raw, self.instrument_pids = self.base.download_instrument(
            "thies-lnm"
        )
        full_paths.sort()
        utils.concatenate_text_files(full_paths, self.base.daily_file.name)
        self.uuid.product = disdrometer2nc(
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


def _unzip_gz_files(full_paths: list) -> str:
    for full_path in full_paths:
        if full_path.endswith(".gz"):
            filename = full_path.replace(".gz", "")
            with gzip.open(full_path, "rb") as file_in:
                with open(filename, "wb") as file_out:
                    shutil.copyfileobj(file_in, file_out)
    return os.path.dirname(full_paths[0])


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
