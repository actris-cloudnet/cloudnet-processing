"""Metadata API for Cloudnet files."""
import os
import shutil
from os import path
import hashlib
import requests
from fastapi import UploadFile, HTTPException
import netCDF4


class ModelDataSubmissionApi:
    """Class handling model data submissions to Cloudnet data portal."""

    def __init__(self, config, session=requests.Session()):
        self.config = config
        self._url = path.join(self.config['METADATASERVER']['url'], 'modelFiles')
        self._session = session
        self._temp_full_path = None

    def save_file_to_temp(self, file_obj: UploadFile) -> None:
        """Save API-submitted model file to /tmp."""
        self._temp_full_path = _save(file_obj, '/tmp')

    def put_metadata(self, payload: dict) -> None:
        """Put submitted Cloudnet model file metadata to database."""
        res = self._session.put(self._url, json=payload)
        if str(res.status_code) not in ('200', '201'):
            os.remove(self._temp_full_path)
            raise HTTPException(status_code=int(res.status_code), detail=res.json())

    def move_file_from_temp(self, payload: dict) -> str:
        """Move model file from temp folder to final destination."""
        dir_name = path.join(self.config['PATH']['received_model_api_files'], payload['location'],
                             'calibrated', payload['modelType'], payload['year'])
        os.makedirs(dir_name, exist_ok=True)
        full_path = path.abspath(path.join(dir_name, payload['filename']))
        shutil.move(self._temp_full_path, full_path)
        return full_path

    def link_file(self, full_path: str) -> None:
        dst = path.join(self.config['PATH']['public'], 'model', path.basename(full_path))
        if not os.path.islink(dst):
            os.symlink(full_path, dst)

    def create_payload(self, meta: dict) -> dict:
        try:
            root_grp = netCDF4.Dataset(self._temp_full_path)
            for key in ('temperature', 'pressure', 'q'):
                assert key in root_grp.variables
        except (OSError, AssertionError):
            os.remove(self._temp_full_path)
            raise HTTPException(status_code=400, detail=f"Invalid model netCDF file: {meta['filename']}")
        global_attrs = {key: str(getattr(root_grp, key)) for key in root_grp.ncattrs()}
        payload = {**global_attrs, **meta,
                   'size': os.stat(self._temp_full_path).st_size,
                   'format': root_grp.data_model}
        root_grp.close()
        payload = self._format_payload(payload)
        return payload

    @staticmethod
    def _format_payload(payload: dict) -> dict:
        if 'location' in payload:
            payload['location'] = payload['location'].lower()
        for key in ('day', 'month'):
            if key in payload:
                payload[key] = payload[key].zfill(2)
        return payload


class DataSubmissionApi:
    """Class handling data submissions to Cloudnet data portal."""

    def __init__(self, config, session=requests.Session()):
        self.config = config
        self._meta_server = self.config['METADATASERVER']['url']
        self._session = session

    def create_url(self, meta: dict) -> str:
        """Create url for metadata submissions."""
        end_point = path.join('metadata', meta['hashSum'][:18])
        return path.join(self._meta_server, end_point)

    def put_metadata(self, url: str, meta: dict) -> None:
        """Put Cloudnet file metadata to database."""
        res = self._session.put(url, json=meta)
        if str(res.status_code) == '200':
            raise HTTPException(status_code=200, detail="File already exists")
        if str(res.status_code) != '201':
            raise HTTPException(status_code=int(res.status_code), detail=res.json())

    def update_metadata_status_to_processed(self, url: str) -> None:
        """Update status of uploaded file."""
        self._session.post(url, json={'status': 'uploaded'})

    def save_file(self, meta: dict, file_obj: UploadFile):
        """Save API-submitted Cloudnet file to local disk."""
        dir_name = self._get_dir(meta)
        os.makedirs(dir_name, exist_ok=True)
        _save(file_obj, dir_name)

    def _get_dir(self, meta: dict) -> str:
        yyyy, mm, dd = meta['measurementDate'].split('-')
        return path.join(self.config['PATH']['received_api_files'], meta['site'], 'uncalibrated',
                         meta['instrument'], yyyy, mm, dd)


def check_hash(expected_hash: str, file_obj: UploadFile) -> None:
    """Check that submitted file matches expected hash."""
    hash_sum = hashlib.sha256()
    for byte_block in iter(lambda: file_obj.file.read(4096), b""):
        hash_sum.update(byte_block)
    file_obj.file.seek(0)
    if hash_sum.hexdigest() != expected_hash:
        raise HTTPException(status_code=400, detail="hashSum does not match sent file")


def _save(file_obj: UploadFile, dir_name: str) -> str:
    full_path = path.join(dir_name, file_obj.filename)
    try:
        with open(full_path, 'wb+') as file:
            shutil.copyfileobj(file_obj.file, file)
    except IOError:
        raise HTTPException(status_code=500, detail=f"File saving failed: {full_path}")
    return full_path
