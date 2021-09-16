from data_processing.processing_tools import Uuid
from cloudnetpy.instruments import rpg2nc, mira2nc, basta2nc, ceilo2nc, hatpro2nc
import gzip
import shutil
import os
import logging
from tempfile import NamedTemporaryFile
from data_processing import concat_wrapper, utils
from data_processing.utils import RawDataMissingError, SkipBlock
from cloudnetpy.utils import is_timestamp


class ProcessInstrument:
    def __init__(self, process_cloudnet, temp_file: NamedTemporaryFile, uuid: Uuid):
        self.base = process_cloudnet
        self.temp_file = temp_file
        self.uuid = uuid
        self._daily_file = NamedTemporaryFile()
        self._kwargs = self._get_kwargs()
        self._args = self._get_args()

    def _get_args(self) -> tuple:
        return self.temp_file.name, self.base.site_meta

    def _get_kwargs(self) -> dict:
        return {'uuid': self.uuid.volatile, 'date': self.base.date_str}

    def _fetch_calibration_factor(self, instrument: str) -> dict:
        meta = self.base.site_meta.copy()
        meta['calibration_factor'] = utils.get_calibration_factor(self.base.site,
                                                                  self.base.date_str,
                                                                  instrument)
        return meta


class ProcessRadar(ProcessInstrument):
    def process_rpg_fmcw_94(self):
        full_paths, raw_uuids = self.base.download_instrument('rpg-fmcw-94', '.lv1$')
        self.uuid.product, valid_full_paths = rpg2nc(self.base.temp_dir.name,
                                                     *self._args,
                                                     **self._kwargs)
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)

    def process_mira(self):
        full_paths, self.uuid.raw = self.base.download_instrument('mira')
        dir_name = _unzip_gz_files(full_paths)
        self.uuid.product = mira2nc(dir_name, *self._args, **self._kwargs)

    def process_basta(self):
        full_path, self.uuid.raw = self.base.download_instrument('basta', largest_only=True)
        self.uuid.product = basta2nc(full_path, *self._args, **self._kwargs)


class ProcessLidar(ProcessInstrument):

    file_id = 'clu-generated-daily'

    def process_chm15k(self):
        full_paths, raw_uuids = self.base.download_instrument('chm15k')
        valid_full_paths = concat_wrapper.concat_chm15k_files(full_paths,
                                                              self.base.date_str,
                                                              self._daily_file.name)
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)
        self._call_ceilo2nc('chm15k')

    def process_ct25k(self):
        full_path, self.uuid.raw = self.base.download_instrument('ct25k', largest_only=True)
        shutil.move(full_path, self._daily_file.name)
        self._call_ceilo2nc('ct25k')

    def process_cl51(self):
        if self.base.site == 'norunda':
            full_paths, self.uuid.raw = self.base.download_adjoining_daily_files('cl51')
            utils.concatenate_text_files(full_paths, self._daily_file.name)
            _fix_cl51_timestamps(self._daily_file.name, 'Europe/Stockholm')
        else:
            full_path, self.uuid.raw = self.base.download_instrument('cl51', largest_only=True)
            shutil.move(full_path, self._daily_file.name)
        self._call_ceilo2nc('cl51')

    def process_halo_doppler_lidar(self):
        full_path, self.uuid.raw = self.base.download_instrument('halo-doppler-lidar',
                                                                 include_pattern='.nc$',
                                                                 largest_only=True)
        self.uuid.product = self.base.fix_calibrated_daily_file(self.uuid, full_path,
                                                                'halo-doppler-lidar')

    def process_cl61d(self):
        model = 'cl61d'
        self._daily_file.name = self._create_daily_file_name(model)
        try:
            if self.base.is_reprocess:
                raise SkipBlock  # Move to next block and re-create daily file
            tmp_file, uuid = self.base.download_instrument(model,
                                                           include_pattern=self.file_id,
                                                           largest_only=True)
            full_paths, raw_uuids = self.base.download_uploaded(model,
                                                                exclude_pattern=self.file_id)
            valid_full_paths = concat_wrapper.update_daily_file(full_paths, tmp_file)
            shutil.copy(tmp_file, self._daily_file.name)
        except (RawDataMissingError, SkipBlock):
            full_paths, raw_uuids = self.base.download_instrument(model,
                                                                  exclude_pattern=self.file_id)
            if full_paths:
                logging.info(f'Creating daily file from {len(full_paths)} files')
            else:
                raise RawDataMissingError
            valid_full_paths = concat_wrapper.concat_netcdf_files(full_paths,
                                                                  self.base.date_str,
                                                                  self._daily_file.name,
                                                                  concat_dimension='profile')
        if not valid_full_paths:
            raise RawDataMissingError
        self.base.md_api.upload_instrument_file(self._daily_file.name,
                                                model,
                                                self.base.date_str,
                                                self.base.site)
        self._call_ceilo2nc(model)
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)

    def _create_daily_file_name(self, model: str) -> str:
        dir_name = os.path.dirname(self._daily_file.name)
        date = self.base.date_str.replace('-', '')
        return f'{dir_name}/{date}_{self.base.site}_{model}_{self.file_id}.nc'

    def _call_ceilo2nc(self, instrument: str):
        site_meta = self._fetch_calibration_factor(instrument)
        self.uuid.product = ceilo2nc(self._daily_file.name,
                                     self.temp_file.name,
                                     site_meta=site_meta,
                                     uuid=self.uuid.volatile,
                                     date=self.base.date_str)


class ProcessMwr(ProcessInstrument):

    def process_hatpro(self):
        try:
            full_paths, raw_uuids = self.base.download_instrument('hatpro', '^(?!.*scan).*\.lwp$')
            self.uuid.product, valid_full_paths = hatpro2nc(self.base.temp_dir.name,
                                                            *self._get_args())
        except RawDataMissingError:
            pattern = '(ufs_l2a.nc$|clwvi.*.nc$|.lwp.*.nc$)'
            full_paths, raw_uuids = self.base.download_instrument('hatpro', pattern)
            valid_full_paths = concat_wrapper.concat_netcdf_files(full_paths,
                                                                  self.base.date_str,
                                                                  self.temp_file.name)
            self.uuid.product = self.base.fix_calibrated_daily_file(self.uuid,
                                                                    self.temp_file.name,
                                                                    'hatpro')
        self.uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)


def _get_valid_uuids(uuids: list, full_paths: list, valid_full_paths: list) -> list:
    return [uuid for uuid, full_path in zip(uuids, full_paths) if full_path in valid_full_paths]


def _unzip_gz_files(full_paths: list) -> str:
    for full_path in full_paths:
        if full_path.endswith('.gz'):
            filename = full_path.replace('.gz', '')
            with gzip.open(full_path, 'rb') as file_in:
                with open(filename, 'wb') as file_out:
                    shutil.copyfileobj(file_in, file_out)
    return os.path.dirname(full_paths[0])


def _fix_cl51_timestamps(filename: str, time_zone: str) -> None:
    with open(filename, 'r') as file:
        lines = file.readlines()
    for ind, line in enumerate(lines):
        if is_timestamp(line):
            date_time = line.strip('-').strip('\n')
            date_time_utc = utils.datetime_to_utc(date_time, time_zone)
            lines[ind] = f'-{date_time_utc}\n'
    with open(filename, 'w') as file:
        file.writelines(lines)
