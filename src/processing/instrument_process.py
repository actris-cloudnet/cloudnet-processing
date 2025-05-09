import dataclasses
import datetime
import gzip
import itertools
import logging
import re
import shutil
from collections import defaultdict
from os import PathLike
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

import doppy
import netCDF4
import numpy as np
from cloudnetpy.instruments import (
    basta2nc,
    bowtie2nc,
    ceilo2nc,
    copernicus2nc,
    galileo2nc,
    hatpro2l1c,
    hatpro2nc,
    instruments,
    mira2nc,
    mrr2nc,
    parsivel2nc,
    pollyxt2nc,
    radiometrics2nc,
    rain_e_h32nc,
    rpg2nc,
    thies2nc,
    ws2nc,
)
from cloudnetpy.utils import is_timestamp
from requests.exceptions import HTTPError

from processing import concat_wrapper, harmonizer
from processing.processor import InstrumentParams, Processor
from processing.utils import RawDataMissingError, Uuid, fetch_calibration, unzip_gz_file


class ProcessInstrument:
    def __init__(
        self,
        directory: Path,
        params: InstrumentParams,
        uuid: Uuid,
        processor: Processor,
    ):
        self.output_path = directory / "output.nc"
        self.daily_path = directory / "daily.nc"
        self.raw_dir = directory / "raw"
        self.raw_dir.mkdir()
        self.uuid = uuid
        self.params = params
        self.processor = processor
        self.site_meta = {
            "name": params.site.name,
            "latitude": params.site.latitude,
            "longitude": params.site.longitude,
            "altitude": params.site.altitude,
        }
        self._kwargs = self._get_kwargs()
        self._args = self._get_args()

    def _get_args(self) -> tuple:
        return str(self.output_path), self.site_meta

    def _get_kwargs(self) -> dict:
        return {"uuid": self.uuid.volatile, "date": self.params.date.isoformat()}

    def _get_payload_for_nc_file_augmenter(self, full_path: str) -> dict:
        return {
            "site_name": self.params.site.id,
            "date": self.params.date.isoformat(),
            "site_meta": self.site_meta,
            "full_path": full_path,
            "output_path": self.output_path,
            "uuid": self.uuid.volatile,
            "instrument": self.params.instrument,
        }

    def download_instrument(
        self,
        include_pattern: str | None = None,
        largest_only: bool = False,
        exclude_pattern: str | None = None,
        include_tag_subset: set[str] | None = None,
        exclude_tag_subset: set[str] | None = None,
        date: datetime.date | tuple[datetime.date, datetime.date] | None = None,
        allow_empty=False,
        filename_prefix: str | None = None,
        filename_suffix: str | None = None,
        subdir: str | None = None,
        time_offset: datetime.timedelta | None = None,
    ):
        directory = self.raw_dir
        if subdir is not None:
            directory = directory / subdir
            directory.mkdir(parents=True, exist_ok=True)
        return self.processor.download_instrument(
            site_id=self.params.site.id,
            date=self.params.date if date is None else date,
            instrument_id=self.params.instrument.type,
            instrument_pid=self.params.instrument.pid,
            directory=directory,
            include_pattern=include_pattern,
            largest_only=largest_only,
            exclude_pattern=exclude_pattern,
            include_tag_subset=include_tag_subset,
            exclude_tag_subset=exclude_tag_subset,
            allow_empty=allow_empty,
            filename_prefix=filename_prefix,
            filename_suffix=filename_suffix,
            time_offset=time_offset,
        )


class ProcessRadar(ProcessInstrument):
    def process_rpg_fmcw_94(self):
        if self.params.site.id == "rv-meteor":
            full_path, self.uuid.raw = self.download_instrument(largest_only=True)
            self.uuid.product = bowtie2nc(full_path, *self._args, **self._kwargs)
        else:
            full_paths, raw_uuids = self.download_instrument(
                include_pattern=r"zen.*\.lv1(\.gz)?$"
            )
            full_paths = _unzip_gz_files(full_paths)
            self.uuid.product, valid_full_paths = rpg2nc(
                str(self.raw_dir), *self._args, **self._kwargs
            )
            self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)

    def process_rpg_fmcw_35(self):
        self.process_rpg_fmcw_94()

    def process_mira_10(self):
        full_paths, self.uuid.raw = self.download_instrument()
        full_paths = _unzip_gz_files(full_paths)
        full_paths = self._fix_suffices(full_paths, ".znc")
        for key in ("azimuth_offset", "zenith_offset", "snr_limit"):
            self._add_calibration(key)
        output_filename, site_meta = self._args
        site_meta["model"] = "mira-10"
        self.uuid.product = mira2nc(
            [str(path) for path in full_paths],
            str(output_filename),
            site_meta,
            **self._kwargs,
        )

    def process_mira_35(self):
        full_paths, self.uuid.raw = self.download_instrument()
        full_paths = _unzip_gz_files(full_paths)
        full_paths = self._fix_suffices(full_paths, ".mmclx")
        for key in ("azimuth_offset", "zenith_offset", "snr_limit"):
            self._add_calibration(key)
        output_filename, site_meta = self._args
        site_meta["model"] = "mira-35"
        self.uuid.product = mira2nc(
            [str(path) for path in full_paths],
            str(output_filename),
            site_meta,
            **self._kwargs,
        )

    def process_basta(self):
        full_path, self.uuid.raw = self.download_instrument(largest_only=True)
        self.uuid.product = basta2nc(full_path, *self._args, **self._kwargs)

    def process_copernicus(self):
        full_paths, self.uuid.raw = self.download_instrument()
        self._add_calibration("range_offset", 0)
        self.uuid.product = copernicus2nc(
            str(self.raw_dir), *self._args, **self._kwargs
        )

    def process_galileo(self):
        full_paths, self.uuid.raw = self.download_instrument()
        self.uuid.product = galileo2nc(str(self.raw_dir), *self._args, **self._kwargs)

    @staticmethod
    def _fix_suffices(full_paths: list[Path], suffix: str) -> list[Path]:
        """Fixes filenames that have incorrect suffix."""
        out_paths = []
        for filename in full_paths:
            if filename.suffix != suffix:
                filename = filename.rename(
                    filename.with_name(f"{filename.name}{suffix}")
                )
            out_paths.append(filename)
        return out_paths

    def _add_calibration(
        self, key: str, default_value: Any = None, api_key: str | None = None
    ) -> None:
        calibration = fetch_calibration(self.params.instrument.pid, self.params.date)
        if calibration is not None:
            data = calibration["data"]
            self.site_meta[key] = data.get(api_key or key, default_value)


class ProcessDopplerLidarWind(ProcessInstrument):
    def process_halo_doppler_lidar(self):
        calibration = fetch_calibration(self.params.instrument.pid, self.params.date)
        time_offset = (
            datetime.timedelta(minutes=calibration["data"]["time_offset"])
            if calibration is not None and "time_offset" in calibration["data"]
            else None
        )
        full_paths, self.uuid.raw = self.download_instrument(
            include_pattern=r".*\.hpl",
            exclude_pattern=r"Stare.*",
            exclude_tag_subset={"cross"},
            time_offset=time_offset,
        )
        try:
            options = self._calibration_options()
            wind = doppy.product.Wind.from_halo_data(data=full_paths, options=options)
        except doppy.exceptions.NoDataError as err:
            raise RawDataMissingError(str(err))
        _doppy_wind_to_nc(
            wind, str(self.daily_path), self.params.date, options, time_offset
        )
        data = self._get_payload_for_nc_file_augmenter(str(self.daily_path))
        if options is not None and options.azimuth_offset_deg is not None:
            data["azimuth_offset_deg"] = options.azimuth_offset_deg
        self.uuid.product = harmonizer.harmonize_doppler_lidar_wind_file(
            data, instruments.HALO
        )

    def process_wls100s(self):
        raise NotImplementedError()

    def process_wls200s(self):
        self._process_windcube(instruments.WINDCUBE_WLS200S)

    def process_wls400s(self):
        self._process_windcube(instruments.WINDCUBE_WLS400S)

    def process_wls70(self):
        full_paths, self.uuid.raw = self.download_instrument(include_pattern=r".*\.rtd")
        try:
            options = self._calibration_options()
            wind = doppy.product.Wind.from_wls70_data(data=full_paths, options=options)
        except doppy.exceptions.NoDataError as err:
            raise RawDataMissingError(str(err))
        _doppy_wls70_wind_to_nc(wind, str(self.daily_path), options)
        data = self._get_payload_for_nc_file_augmenter(str(self.daily_path))
        if options is not None and options.azimuth_offset_deg is not None:
            data["azimuth_offset_deg"] = options.azimuth_offset_deg
        self.uuid.product = harmonizer.harmonize_doppler_lidar_wind_file(
            data, instruments.WINDCUBE_WLS70
        )

    def _process_windcube(self, instrument: instruments.Instrument):
        file_groups = defaultdict(list)
        full_paths, self.uuid.raw = self.download_instrument(
            include_pattern=r".*(vad)|(dbs).*\.nc.*",
        )
        full_paths = _unzip_gz_files(full_paths)
        vad_paths = [p for p in full_paths if re.match(r".*_vad_(.*)\.nc\.*", p.name)]
        if vad_paths:
            group_pattern = re.compile(r".*_vad_(.*)\.nc\.*")
        else:
            group_pattern = re.compile(r".*_dbs_(.*)\.nc\.*")
        for path, uuid in zip(full_paths, self.uuid.raw):
            if match := group_pattern.match(path.name):
                file_groups[match.group(1)].append((path, uuid))
        if not file_groups:
            raise RawDataMissingError("No valid files found in the download.")

        try:
            options = self._calibration_options()
            try:
                # try first with files with all subgroups
                all_grouped_files = list(itertools.chain(*file_groups.values()))
                all_paths, self.uuid.raw = zip(*all_grouped_files)  # type: ignore
                wind = doppy.product.Wind.from_windcube_data(
                    data=all_paths, options=options
                )
            except ValueError:
                group_with_most_files = max(
                    file_groups, key=lambda k: len(file_groups[k])
                )
                full_paths, self.uuid.raw = zip(*file_groups[group_with_most_files])  # type: ignore
                wind = doppy.product.Wind.from_windcube_data(
                    data=full_paths, options=options
                )
        except doppy.exceptions.NoDataError as err:
            raise RawDataMissingError(str(err))
        _doppy_wind_to_nc(wind, str(self.daily_path), self.params.date, options)
        data = self._get_payload_for_nc_file_augmenter(str(self.daily_path))
        if options is not None and options.azimuth_offset_deg is not None:
            data["azimuth_offset_deg"] = options.azimuth_offset_deg
        self.uuid.product = harmonizer.harmonize_doppler_lidar_wind_file(
            data, instrument
        )

    def _calibration_options(self) -> doppy.product.WindOptions | None:
        calibration = fetch_calibration(self.params.instrument.pid, self.params.date)
        azimuth_offset = (
            calibration.get("data", {}).get("azimuth_offset") if calibration else None
        )
        return (
            doppy.product.WindOptions(azimuth_offset_deg=float(azimuth_offset))
            if azimuth_offset
            else None
        )


class ProcessDopplerLidar(ProcessInstrument):
    def process_halo_doppler_lidar(self):
        calibration = fetch_calibration(self.params.instrument.pid, self.params.date)
        time_offset = (
            datetime.timedelta(minutes=calibration["data"]["time_offset"])
            if calibration is not None and "time_offset" in calibration["data"]
            else None
        )
        # Co files either have "co" tag or no tags at all.
        full_paths_co, raw_uuids_co = self.download_instrument(
            filename_prefix="Stare",
            filename_suffix=".hpl",
            exclude_tag_subset={"cross"},
            subdir="co",
            time_offset=time_offset,
        )
        # Cross files should always have "cross" tag.
        full_paths_cross, raw_uuids_cross = self.download_instrument(
            filename_prefix="Stare",
            filename_suffix=".hpl",
            include_tag_subset={"cross"},
            allow_empty=True,
            subdir="cross",
            time_offset=time_offset,
        )
        # Background files usually have no tags or "co" tag. Sometimes they have
        # "co" and "cross" tags randomly because the instrument may save the
        # files like this.
        full_paths_bg, _ = self.download_instrument(
            filename_prefix="Background",
            filename_suffix=".txt",
            date=(
                self.params.date - datetime.timedelta(days=1),
                self.params.date + datetime.timedelta(days=1),
            ),
            subdir="bg",
        )
        self.uuid.raw = raw_uuids_co + raw_uuids_cross

        stare: doppy.product.Stare | doppy.product.StareDepol
        try:
            stare = doppy.product.Stare.from_halo_data(
                data=full_paths_co,
                data_bg=full_paths_bg,
                bg_correction_method=doppy.options.BgCorrectionMethod.FIT,
            )
            if full_paths_cross:
                stare_cross = doppy.product.Stare.from_halo_data(
                    data=full_paths_cross,
                    data_bg=full_paths_bg,
                    bg_correction_method=doppy.options.BgCorrectionMethod.FIT,
                )
                stare = doppy.product.StareDepol(stare, stare_cross)
        except doppy.exceptions.NoDataError as err:
            raise RawDataMissingError(str(err))

        _doppy_stare_to_nc(stare, str(self.daily_path), self.params.date, time_offset)
        data = self._get_payload_for_nc_file_augmenter(str(self.daily_path))
        self.uuid.product = harmonizer.harmonize_doppler_lidar_stare_file(
            data, instruments.HALO
        )

    def process_wls100s(self):
        raise NotImplementedError()

    def process_wls200s(self):
        self._process_windcube(instruments.WINDCUBE_WLS200S)

    def process_wls400s(self):
        self._process_windcube(instruments.WINDCUBE_WLS400S)

    def process_wls70(self):
        raise NotImplementedError()

    def _process_windcube(self, instrument: instruments.Instrument):
        file_groups = defaultdict(list)
        full_paths, self.uuid.raw = self.download_instrument(
            include_pattern=r".*fixed.*\.nc(\..+)?"
        )
        full_paths = _unzip_gz_files(full_paths)

        try:
            # Try to process all "fixed" files at once.
            stare = doppy.product.Stare.from_windcube_data(full_paths)
        except doppy.exceptions.NoDataError as err:
            raise RawDataMissingError(str(err))
        except ValueError:
            # If processing all files fails, try to process individual groups
            # (e.g. "fixed_1633_75m"), starting from the group with most files.
            group_pattern = re.compile(r".+_fixed_(.+)\.nc(?:\..+)?")
            for path, uuid in zip(full_paths, self.uuid.raw):
                if match := group_pattern.match(path.name):
                    file_groups[match.group(1)].append((path, uuid))
            groups_by_size = sorted(
                file_groups, key=lambda k: (len(file_groups[k]), k), reverse=True
            )
            for i, group in enumerate(groups_by_size):
                is_last_group = i == len(groups_by_size) - 1
                try:
                    full_paths, self.uuid.raw = zip(*file_groups[group])  # type: ignore
                    stare = doppy.product.Stare.from_windcube_data(full_paths)
                    threshold = 15
                    zenith_angle = 90 - np.median(stare.elevation)
                    if zenith_angle > threshold:
                        raise ValueError(f"Invalid zenith angle {zenith_angle}")
                    break
                except doppy.exceptions.NoDataError as err:
                    if is_last_group:
                        raise RawDataMissingError(str(err))
                except ValueError:
                    if is_last_group:
                        raise

        _doppy_stare_to_nc(stare, str(self.daily_path), self.params.date)
        data = self._get_payload_for_nc_file_augmenter(str(self.daily_path))
        self.uuid.product = harmonizer.harmonize_doppler_lidar_stare_file(
            data, instrument
        )


class ProcessLidar(ProcessInstrument):
    def process_cs135(self):
        full_paths, self.uuid.raw = self.download_instrument()
        full_paths.sort()
        _concatenate_text_files(full_paths, str(self.daily_path))
        self._call_ceilo2nc("cs135")

    def process_chm15k(self):
        self._process_chm_lidar("chm15k")

    def process_chm15x(self):
        self._process_chm_lidar("chm15x")

    def process_chm15kx(self):
        self._process_chm_lidar("chm15kx")

    def _process_chm_lidar(self, model: str):
        full_paths, raw_uuids = self.download_instrument()
        valid_full_paths = concat_wrapper.concat_chm15k_files(
            full_paths, self.params.date.isoformat(), str(self.daily_path)
        )
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)
        _check_chm_version(str(self.daily_path), model)
        self._call_ceilo2nc("chm15k")

    def process_ct25k(self):
        full_paths, self.uuid.raw = self.download_instrument()
        full_paths.sort()
        full_paths = _unzip_gz_files(full_paths)
        _concatenate_text_files(full_paths, str(self.daily_path))
        self._call_ceilo2nc("ct25k")

    def process_halo_doppler_lidar_calibrated(self):
        full_path, self.uuid.raw = self.download_instrument(largest_only=True)
        data = self._get_payload_for_nc_file_augmenter(full_path)
        self.uuid.product = harmonizer.harmonize_halo_calibrated_file(data)

    def process_pollyxt(self):
        full_paths, self.uuid.raw = self.download_instrument()
        calibration = self._fetch_pollyxt_calibration()
        site_meta = self.site_meta | calibration
        self.uuid.product = pollyxt2nc(
            str(self.raw_dir),
            str(self.output_path),
            site_meta=site_meta,
            uuid=self.uuid.volatile,
            date=self.params.date.isoformat(),
        )

    def process_minimpl(self):
        raise NotImplementedError()

    def process_ld40(self):
        raise NotImplementedError()

    def process_cl31(self):
        full_paths, self.uuid.raw = self.download_instrument()
        full_paths.sort()
        _concatenate_text_files(full_paths, str(self.daily_path))
        self._call_ceilo2nc("cl31")

    def process_cl51(self):
        calibration = fetch_calibration(self.params.instrument.pid, self.params.date)
        time_offset = (
            datetime.timedelta(minutes=calibration["data"]["time_offset"])
            if calibration is not None and "time_offset" in calibration["data"]
            else None
        )
        full_paths, self.uuid.raw = self.download_instrument(time_offset=time_offset)
        full_paths.sort()
        _concatenate_text_files(full_paths, str(self.daily_path))
        if time_offset is not None:
            logging.info(
                "Shifting timestamps to UTC by %d minutes",
                time_offset / datetime.timedelta(minutes=1),
            )
            _fix_cl51_timestamps(str(self.daily_path), time_offset)
        self._call_ceilo2nc("cl51")

    def process_cl61d(self):
        full_paths, raw_uuids = self.download_instrument(
            exclude_pattern="clu-generated"
        )
        variables = ["x_pol", "p_pol", "beta_att", "time", "tilt_angle"]
        try:
            valid_full_paths = concat_wrapper.concat_netcdf_files(
                full_paths,
                self.params.date.isoformat(),
                str(self.daily_path),
                variables=variables,
            )
        except KeyError:
            valid_full_paths = concat_wrapper.concat_netcdf_files(
                full_paths,
                self.params.date.isoformat(),
                str(self.daily_path),
                concat_dimension="profile",
                variables=variables,
            )
        if not valid_full_paths:
            raise RawDataMissingError()
        self._call_ceilo2nc("cl61d")
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)

    def _call_ceilo2nc(self, model: str):
        calibration = self._fetch_ceilo_calibration()
        site_meta = self.site_meta | calibration
        site_meta["model"] = model
        self.uuid.product = ceilo2nc(
            str(self.daily_path),
            str(self.output_path),
            site_meta=site_meta,
            uuid=self.uuid.volatile,
            date=self.params.date.isoformat(),
        )

    def _fetch_ceilo_calibration(self) -> dict:
        output: dict = {}
        calibration = fetch_calibration(self.params.instrument.pid, self.params.date)
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
        calibration = fetch_calibration(self.params.instrument.pid, self.params.date)
        if not calibration:
            return output
        if "snr_limit" in calibration["data"]:
            output["snr_limit"] = float(calibration["data"]["snr_limit"])
        return output


class ProcessMwrL1c(ProcessInstrument):
    def process_hatpro(self):
        self._process_rpg("hatpro")

    def process_lhatpro(self):
        self._process_rpg("lhatpro")

    def process_lhumpro(self):
        self._process_rpg("lhumpro_u90")

    def _process_rpg(
        self, instrument_type: Literal["hatpro", "lhatpro", "lhumpro_u90"]
    ):
        calibration = self._get_calibration_data()
        full_paths, self.uuid.raw = self.download_instrument(
            include_pattern=r"\.(brt|hkd|met|irt|blb|bls)$",
            exclude_pattern=r"2DSCAN|elscan",
            time_offset=calibration.get("time_offset"),
        )
        output_filename, site_meta = self._args

        site_meta = {**site_meta, **calibration}
        coeff_paths = []
        for link in calibration["coefficientLinks"]:
            filename = link.split("/")[-1]
            full_path = self.raw_dir / filename
            with open(full_path, "wb") as f:
                res = self.processor.md_api.session.get(link)
                res.raise_for_status()
                f.write(res.content)
            coeff_paths.append(str(full_path))
        site_meta["coefficientFiles"] = coeff_paths

        self.uuid.product = hatpro2l1c(
            str(self.raw_dir),
            str(output_filename),
            site_meta,
            instrument_type=instrument_type,
            **self._kwargs,
        )

    def _get_calibration_data(self) -> dict:
        payload = {
            "date": self.params.date.isoformat(),
            "instrumentPid": self.params.instrument.pid,
        }
        try:
            res = self.processor.md_api.get("api/calibration", payload)
        except HTTPError:
            raise RawDataMissingError("Skipping due to missing mwrpy coefficients")
        data = res["data"]
        if "time_offset" in data:
            data["time_offset"] = datetime.timedelta(minutes=data["time_offset"])
        return data


class ProcessMwr(ProcessInstrument):
    def process_hatpro(self):
        self._process_rpg("hatpro")

    def process_lhatpro(self):
        self._process_rpg("lhatpro")

    def process_lhumpro(self):
        self._process_rpg("lhumpro_u90")

    def process_radiometrics(self):
        full_paths, self.uuid.raw = self.download_instrument()
        _unzip_gz_files(full_paths)
        self.uuid.product = radiometrics2nc(
            str(self.raw_dir), *self._args, **self._kwargs
        )

    def _process_rpg(
        self, instrument_type: Literal["hatpro", "lhatpro", "lhumpro_u90"]
    ):
        try:
            full_paths, raw_uuids = self.download_instrument(
                include_pattern=r"\.(lwp|iwv)$",
                exclude_pattern=r"2DSCAN",
            )
            kwargs = {**self._kwargs, "instrument_type": instrument_type}
            self.uuid.product, valid_full_paths = hatpro2nc(
                str(self.raw_dir),
                *self._args,
                **kwargs,
            )

        except RawDataMissingError:
            full_paths, raw_uuids = self.download_instrument(
                include_pattern="(ufs_l2a.nc$|clwvi.*.nc$|.lwp.*.nc$)"
            )
            valid_full_paths = concat_wrapper.concat_netcdf_files(
                full_paths, self.params.date.isoformat(), str(self.daily_path)
            )
            data = self._get_payload_for_nc_file_augmenter(str(self.daily_path))
            self.uuid.product = harmonizer.harmonize_hatpro_file(data)
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)


class ProcessDisdrometer(ProcessInstrument):
    def process_parsivel(self):
        try:
            full_paths, self.uuid.raw = self.download_instrument(largest_only=True)
            data = self._get_payload_for_nc_file_augmenter(full_paths)
            self.uuid.product = harmonizer.harmonize_parsivel_file(data)
        except OSError:
            full_paths, self.uuid.raw = self.download_instrument()
            calibration = self._fetch_parsivel_calibration()
            kwargs = self._kwargs.copy()
            kwargs["telegram"] = calibration["telegram"]
            if calibration["missing_timestamps"] is True:
                full_paths, timestamps = _deduce_parsivel_timestamps(full_paths)
                kwargs["timestamps"] = timestamps
            # Add missing semicolon between timestamp and serial number (450416)
            if self.params.site.id == "norunda":
                for path in full_paths:
                    text = path.read_bytes()
                    new_text = re.sub(
                        rb"^(\d{14}\.\d{3})450416;",
                        rb"\1;450416;",
                        text,
                        flags=re.MULTILINE,
                    )
                    path.write_bytes(new_text)
            # Fix Jülich files.
            if self.params.instrument.uuid == "27837d00-b6af-4e90-baa0-f670f31abcd0":
                for path in full_paths:
                    with open(path, "rb+") as f:
                        lines = []
                        for line in f:
                            # Support timestamp within angle brackets.
                            line = re.sub(rb"<(\d{14}\.\d{3})>", rb"\1;", line)

                            # Line starts with timestamp but also contains
                            # another timestamp at random location.
                            m = re.match(rb"\d{8}\d{6}\.\d{3};", line)
                            if m is not None:
                                line = line[:8] + re.sub(
                                    line[:8] + rb"\d{6}\.\d{3};", b"", line[8:]
                                )
                            lines.append(line)

                        f.seek(0)
                        f.truncate()
                        f.writelines(lines)
            self.uuid.product = parsivel2nc(full_paths, *self._args, **kwargs)

    def process_thies_lnm(self):
        full_paths, self.uuid.raw = self.download_instrument()
        full_paths.sort()
        full_paths = _unzip_gz_files(full_paths)
        _concatenate_text_files(full_paths, self.daily_path)
        site_meta = self.site_meta.copy()
        if self.params.site.id == "leipzig-lim":
            site_meta["truncate_columns"] = 23
        self.uuid.product = thies2nc(
            str(self.daily_path), str(self.output_path), site_meta, **self._kwargs
        )

    def _fetch_parsivel_calibration(self) -> dict:
        output: dict = {"telegram": None, "missing_timestamps": False}
        calibration = fetch_calibration(self.params.instrument.pid, self.params.date)
        if calibration is not None:
            data = calibration["data"]
            output["telegram"] = data.get("telegram", None)
            output["missing_timestamps"] = data.get("missing_timestamps", False)
        return output


class ProcessRainGauge(ProcessInstrument):
    def process_pluvio(self):
        full_path, self.uuid.raw = self.download_instrument(largest_only=True)
        data = self._get_payload_for_nc_file_augmenter(full_path)
        self.uuid.product = harmonizer.harmonize_pluvio_nc(data)

    def process_thies_precipitation_transmitter(self):
        full_path, self.uuid.raw = self.download_instrument(largest_only=True)
        data = self._get_payload_for_nc_file_augmenter(full_path)
        self.uuid.product = harmonizer.harmonize_thies_pt_nc(data)

    def process_rain_e_h3(self):
        full_path, self.uuid.raw = self.download_instrument(largest_only=True)
        self.uuid.product = rain_e_h32nc(full_path, *self._args, **self._kwargs)


class ProcessWeatherStation(ProcessInstrument):
    def process_weather_station(self):
        supported_sites = (
            "palaiseau",
            "lindenberg",
            "granada",
            "kenttarova",
            "hyytiala",
            "bucharest",
            "galati",
            "munich",
            "juelich",
            "lampedusa",
            "limassol",
        )
        if self.params.site.id not in supported_sites:
            raise NotImplementedError("Weather station not implemented for this site")

        calibration = fetch_calibration(self.params.instrument.pid, self.params.date)
        if calibration and calibration.get("data", {}).get("time_offset"):
            time_offset = datetime.timedelta(minutes=calibration["data"]["time_offset"])
            full_paths, self.uuid.raw = self.download_instrument(
                time_offset=time_offset
            )
            full_paths.sort()
            self.uuid.product = ws2nc(
                [str(path) for path in full_paths], *self._args, **self._kwargs
            )
        else:
            full_path, self.uuid.raw = self.download_instrument(largest_only=True)
            if self.params.site.id in ("lindenberg", "munich"):
                data = self._get_payload_for_nc_file_augmenter(full_path)
                self.uuid.product = harmonizer.harmonize_ws_file(data)
            else:
                self.uuid.product = ws2nc(str(full_path), *self._args, **self._kwargs)


class ProcessRainRadar(ProcessInstrument):
    def process_mrr_pro(self):
        full_paths, self.uuid.raw = self.download_instrument()
        full_paths = _unzip_gz_files(full_paths)
        self.uuid.product = mrr2nc(full_paths, *self._args, **self._kwargs)


def _get_valid_uuids(
    uuids: list[UUID], full_paths: list[Path], valid_full_paths: list[str]
) -> list[UUID]:
    valid_paths = [Path(path) for path in valid_full_paths]
    return [
        uuid for uuid, full_path in zip(uuids, full_paths) if full_path in valid_paths
    ]


def _unzip_gz_files(full_paths: list[Path]) -> list[Path]:
    paths_out = []
    for path_in in full_paths:
        try:
            paths_out.append(unzip_gz_file(path_in))
        except (EOFError, gzip.BadGzipFile) as err:
            logging.warning("Cannot unzip gz file %s: %s", path_in, err)
    return paths_out


def _fix_cl51_timestamps(filename: str, offset: datetime.timedelta) -> None:
    with open(filename, "r") as file:
        lines = file.readlines()
    for ind, line in enumerate(lines):
        if is_timestamp(line):
            date_time = line.strip("-").strip("\n")
            date_time_utc = _shift_datetime(date_time, offset)
            lines[ind] = f"-{date_time_utc}\n"
    with open(filename, "w") as file:
        file.writelines(lines)


def _doppy_stare_to_nc(
    stare: doppy.product.Stare | doppy.product.StareDepol,
    filename: str,
    date: datetime.date,
    time_offset: datetime.timedelta | None = None,
) -> None:
    n_time = len(stare.time)
    if time_offset is not None:
        stare.time -= np.timedelta64(time_offset)
    is_valid = stare.time.astype("datetime64[D]") == np.datetime64(date)
    if not np.any(is_valid):
        raise RawDataMissingError("No valid timestamps found")
    for key, value in dataclasses.asdict(stare).items():
        if isinstance(value, np.ndarray) and value.shape[:1] == (n_time,):
            setattr(stare, key, value[is_valid])
    with doppy.netcdf.Dataset(filename, format="NETCDF4_CLASSIC") as nc:
        nc.add_dimension("time")
        nc.add_dimension("range", size=len(stare.radial_distance))
        nc.add_time(
            name="time",
            dimensions=("time",),
            standard_name="time",
            long_name="Time UTC",
            data=stare.time,
            dtype="f8",
        )
        nc.add_variable(
            name="range",
            dimensions=("range",),
            units="m",
            data=stare.radial_distance,
            dtype="f4",
        )
        nc.add_variable(
            name="elevation",
            dimensions=("time",),
            units="degrees",
            data=stare.elevation,
            dtype="f4",
            long_name="Elevation from horizontal",
        )
        nc.add_variable(
            name="beta_raw",
            dimensions=("time", "range"),
            units="sr-1 m-1",
            data=stare.beta,
            dtype="f4",
        )
        nc.add_variable(
            name="beta",
            dimensions=("time", "range"),
            units="sr-1 m-1",
            data=stare.beta,
            dtype="f4",
            mask=stare.mask_beta,
        )
        nc.add_variable(
            name="v",
            dimensions=("time", "range"),
            units="m s-1",
            long_name="Doppler velocity",
            data=stare.radial_velocity,
            dtype="f4",
            mask=stare.mask_radial_velocity,
        )
        nc.add_scalar_variable(
            name="wavelength",
            units="m",
            standard_name="radiation_wavelength",
            data=stare.wavelength,
            dtype="f4",
        )
        if isinstance(stare, doppy.product.StareDepol):
            nc.add_variable(
                name="depolarisation_raw",
                dimensions=("time", "range"),
                units="1",
                data=stare.depolarisation,
                dtype="f4",
                mask=stare.mask_depolarisation,
            )
            nc.add_variable(
                name="depolarisation",
                dimensions=("time", "range"),
                units="1",
                data=stare.depolarisation,
                dtype="f4",
                mask=stare.mask_beta | stare.mask_depolarisation,
            )
            nc.add_variable(
                name="beta_cross_raw",
                dimensions=("time", "range"),
                units="sr-1 m-1",
                data=stare.beta_cross,
                mask=stare.mask_beta_cross,
                dtype="f4",
            )
            nc.add_variable(
                name="beta_cross",
                dimensions=("time", "range"),
                units="sr-1 m-1",
                data=stare.beta_cross,
                mask=stare.mask_beta | stare.mask_beta_cross,
                dtype="f4",
            )
            nc.add_scalar_variable(
                name="polariser_bleed_through",
                units="1",
                long_name="Polariser bleed-through",
                data=stare.polariser_bleed_through,
                dtype="f4",
            )
        nc.add_attribute("serial_number", stare.system_id)
        nc.add_attribute("doppy_version", doppy.__version__)
        match stare.ray_info:
            case doppy.product.stare.RayAccumulationTime(value):
                nc.add_scalar_variable(
                    name="ray_accumulation_time",
                    units="s",
                    long_name="Ray accumulation time",
                    data=value,
                    dtype="f4",
                )
            case doppy.product.stare.PulsesPerRay(value):
                nc.add_scalar_variable(
                    name="pulses_per_ray",
                    units="1",
                    long_name="Pulses per ray",
                    data=value,
                    dtype="i4",
                )


def _doppy_wind_to_nc(
    wind: doppy.product.Wind,
    filename: str,
    date: datetime.date,
    options: doppy.product.WindOptions | None,
    time_offset: datetime.timedelta | None = None,
) -> None:
    n_time = len(wind.time)
    if time_offset is not None:
        wind.time -= np.timedelta64(time_offset)
    is_valid = wind.time.astype("datetime64[D]") == np.datetime64(date)
    if not np.any(is_valid):
        raise RawDataMissingError("No valid timestamps found")
    for key, value in dataclasses.asdict(wind).items():
        if isinstance(value, np.ndarray) and value.shape[:1] == (n_time,):
            setattr(wind, key, value[is_valid])
    with doppy.netcdf.Dataset(filename, format="NETCDF4_CLASSIC") as nc:
        nc.add_dimension("time")
        nc.add_dimension("height", size=len(wind.height))
        nc.add_time(
            name="time",
            dimensions=("time",),
            standard_name="time",
            long_name="Time UTC",
            data=wind.time,
            dtype="f8",
        )
        nc.add_variable(
            name="height",
            dimensions=("height",),
            units="m",
            data=wind.height,
            dtype="f4",
        )
        nc.add_variable(
            name="uwind_raw",
            dimensions=("time", "height"),
            units="m s-1",
            data=wind.zonal_wind,
            mask=wind.mask_zonal_wind,
            dtype="f4",
            long_name="Non-screened zonal wind",
        )
        nc.add_variable(
            name="uwind",
            dimensions=("time", "height"),
            units="m s-1",
            data=wind.zonal_wind,
            mask=wind.mask | wind.mask_zonal_wind,
            dtype="f4",
            long_name="Zonal wind",
        )
        nc.add_variable(
            name="vwind_raw",
            dimensions=("time", "height"),
            units="m s-1",
            data=wind.meridional_wind,
            mask=wind.mask_meridional_wind,
            dtype="f4",
            long_name="Non-screened meridional wind",
        )
        nc.add_variable(
            name="vwind",
            dimensions=("time", "height"),
            units="m s-1",
            data=wind.meridional_wind,
            mask=wind.mask | wind.mask_meridional_wind,
            dtype="f4",
            long_name="Meridional wind",
        )
        nc.add_attribute("serial_number", wind.system_id)
        nc.add_attribute("doppy_version", doppy.__version__)
        if options is not None and options.azimuth_offset_deg is not None:
            nc.add_scalar_variable(
                name="azimuth_offset",
                units="degrees",
                data=options.azimuth_offset_deg,
                dtype="f4",
                long_name="Azimuth offset of the instrument (positive clockwise from north)",
            )


def _doppy_wls70_wind_to_nc(
    wind: doppy.product.Wind, filename: str, options: doppy.product.WindOptions | None
) -> None:
    with doppy.netcdf.Dataset(filename, format="NETCDF4_CLASSIC") as nc:
        nc.add_dimension("time")
        nc.add_dimension("height", size=len(wind.height))
        nc.add_time(
            name="time",
            dimensions=("time",),
            standard_name="time",
            long_name="Time UTC",
            data=wind.time,
            dtype="f8",
        )
        nc.add_variable(
            name="height",
            dimensions=("height",),
            units="m",
            data=wind.height,
            dtype="f4",
        )
        nc.add_variable(
            name="uwind",
            dimensions=("time", "height"),
            units="m s-1",
            data=wind.zonal_wind,
            mask=wind.mask,
            dtype="f4",
            long_name="Zonal wind",
        )
        nc.add_variable(
            name="vwind",
            dimensions=("time", "height"),
            units="m s-1",
            data=wind.meridional_wind,
            mask=wind.mask,
            dtype="f4",
            long_name="Meridional wind",
        )
        nc.add_attribute("serial_number", wind.system_id)
        nc.add_attribute("doppy_version", doppy.__version__)
        if options is not None and options.azimuth_offset_deg is not None:
            nc.add_scalar_variable(
                name="azimuth_offset",
                units="degrees",
                data=options.azimuth_offset_deg,
                dtype="f4",
                long_name="Azimuth offset of the instrument (positive clockwise from north)",
            )


def _shift_datetime(date_time: str, offset: datetime.timedelta) -> str:
    """Shifts datetime N hours."""
    dt = datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
    dt = dt - offset
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _concatenate_text_files(filenames: list, output_filename: str | PathLike) -> None:
    """Concatenates text files."""
    with open(output_filename, "wb") as target:
        for filename in filenames:
            with open(filename, "rb") as source:
                shutil.copyfileobj(source, target)


def _deduce_parsivel_timestamps(
    file_paths: list[Path],
) -> tuple[list[Path], list[datetime.datetime]]:
    time_stamps, valid_files = [], []
    min_measurements_per_hour = 55
    for filename in sorted(file_paths):
        date = _parse_datetime_from_filename(filename)
        n_lines = _count_lines(filename)
        if not date or n_lines < min_measurements_per_hour:
            logging.info(
                "Expected at least %d measurements but found only %d in %s",
                min_measurements_per_hour,
                n_lines,
                filename.name,
            )
            continue
        start_datetime = datetime.datetime(date[0], date[1], date[2], date[3])
        time_interval = datetime.timedelta(minutes=60 / n_lines)
        datetime_stamps = [start_datetime + time_interval * i for i in range(n_lines)]
        time_stamps.extend(datetime_stamps)
        valid_files.append(filename)
    return valid_files, time_stamps


def _parse_datetime_from_filename(filename: Path) -> list[int] | None:
    pattern = r"(20\d{2})(\d{2})(\d{2})(\d{2})"
    match = re.search(pattern, filename.name)
    if not match:
        return None
    return [int(x) for x in match.groups()]


def _count_lines(filename: Path) -> int:
    with open(filename, "rb") as file:
        n_lines = 0
        for _ in file:
            n_lines += 1
    return n_lines


def _check_chm_version(filename: str, identifier: str):
    def print_warning(expected: str):
        logging.warning(
            f"{expected} data submitted with incorrect identifier {identifier}"
        )

    with netCDF4.Dataset(filename) as nc:
        source = getattr(nc, "source", "")[:3].lower()
    match source, identifier:
        case "chx", "chm15x" | "chm15k":
            print_warning("chm15kx")
        case "chm", "chm15x" | "chm15kx":
            print_warning("chm15k")
