"""Metadata API for Cloudnet files."""
import os
import shutil
from os import path
import hashlib
import ntpath
import requests
from fastapi import UploadFile, HTTPException
import subprocess


class DataSubmissionApi:
    """Class handling data submissions to Cloudnet data portal."""

    def __init__(self, config, session=requests.Session()):
        self.config = config
        self._meta_server = self.config['METADATASERVER']['url']
        self._session = session

    def put_metadata(self, meta: dict) -> None:
        """Put Cloudnet file metadata to database."""
        url = self._construct_url_from_meta(meta)
        res = self._session.put(url, json=meta)
        if str(res.status_code) == '200':
            raise HTTPException(status_code=200, detail="File already exists")
        if str(res.status_code) != '201':
            raise HTTPException(status_code=int(res.status_code), detail=res.json())

    def put_model_metadata(self, meta: dict, file_obj: UploadFile, freeze: bool = False):
        """Put Cloudnet model file metadata to database."""
        payload = subprocess.check_output(['ncdump', '-xh', path.realpath(file_obj.filename)])
        url = path.join(self._meta_server, 'modelFiles')
        headers = {'Content-Type': 'application/xml'}
        if freeze:
            headers['X-Freeze'] = 'True'
        res = self._session.put(url, data=payload, headers=headers)
        res.raise_for_status()
        return res

    def update_metadata_status_to_processed(self, meta: dict) -> None:
        """Update status of uploaded file."""
        url = self._construct_url_from_meta(meta)
        self._session.post(url, json={'status': 'uploaded'})

    def save_file(self, meta: dict, file_obj: UploadFile, model_file: bool = False) -> None:
        """Save API-submitted file (Cloudnet or model) to local disk."""
        if model_file:
            folder = self._get_model_folder(meta)
        else:
            folder = self._get_folder(meta)
        os.makedirs(folder, exist_ok=True)
        self._save(file_obj, folder)

    @staticmethod
    def check_hash(meta: dict, file_obj: UploadFile) -> None:
        """Check that submitted file matches expected hash."""
        hash_sum = hashlib.sha256()
        for byte_block in iter(lambda: file_obj.file.read(4096), b""):
            hash_sum.update(byte_block)
        file_obj.file.seek(0)
        if hash_sum.hexdigest() != meta['hashSum']:
            raise HTTPException(status_code=400, detail="hashSum does not match sent file")

    def _get_folder(self, meta: dict) -> str:
        yyyy, mm, dd = meta['measurementDate'].split('-')
        return path.join(self.config['PATH']['received_api_files'], meta['site'], 'uncalibrated',
                         meta['instrument'], yyyy, mm, dd)

    def _get_model_folder(self, meta: dict) -> str:
        yyyy = meta['date'][:4]
        return path.join(self.config['PATH']['received_api_files'], meta['site'], 'npw_model',
                         meta['modelType'], yyyy)

    def _construct_url_from_meta(self, meta: dict) -> str:
        metadata_id = meta['hashSum'][:18]
        return path.join(self._meta_server, 'metadata', metadata_id)

    @staticmethod
    def _save(file_obj: UploadFile, folder: str) -> None:
        filename = ntpath.basename(file_obj.filename)
        try:
            with open(path.join(folder, filename), 'wb+') as file:
                shutil.copyfileobj(file_obj.file, file)
        except IOError:
            raise HTTPException(status_code=500, detail="File saving failed")
