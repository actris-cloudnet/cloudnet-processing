"""Metadata API for Cloudnet files."""
import os
import shutil
from os import path
import hashlib
import ntpath
import requests
from fastapi import UploadFile, HTTPException


class DataSubmissionApi:
    """Class handling data submissions to Cloudnet data portal."""

    def __init__(self, config, session=requests.Session()):
        self.config = config
        self._meta_server = self.config['METADATASERVER']['url']
        self._session = session

    def put_metadata(self, url: str, meta: dict) -> None:
        res = self._session.put(url, json=meta)
        if str(res.status_code) == '200':
            raise HTTPException(status_code=200, detail="File already exists")
        if str(res.status_code) != '201':
            raise HTTPException(status_code=int(res.status_code), detail=res.json())

    def update_metadata_status_to_processed(self, url: str) -> None:
        self._session.post(url, json={'status': 'uploaded'})

    def save_file(self, meta: dict, file_obj: UploadFile, model_file: bool = False) -> None:
        if model_file:
            folder = self._create_model_folder(meta)
        else:
            folder = self._create_folder(meta)
        self._save(file_obj, folder)

    def _create_folder(self, meta: dict) -> str:
        root = path.join(self.config['PATH']['received_api_files'], meta['site'], 'uncalibrated', meta['instrument'])
        yyyy, mm, dd = meta['measurementDate'].split('-')
        full_path = path.join(root, yyyy, mm, dd)
        os.makedirs(full_path, exist_ok=True)
        return full_path

    def _create_model_folder(self, meta: dict) -> str:
        yyyy = meta['measurementDate'][:4]
        full_path = path.join(self.config['PATH']['received_api_files'], meta['site'],
                              'npw_model', meta['modelType'], yyyy)
        os.makedirs(full_path, exist_ok=True)
        return full_path

    def construct_url_from_meta(self, meta: dict, model_file: bool = False) -> str:
        end_point = 'modelMetadata' if model_file else 'metadata'
        metadata_id = meta['hashSum'][:18]
        return path.join(self._meta_server, end_point, metadata_id)

    @staticmethod
    def check_hash(meta: dict, file_obj: UploadFile) -> None:
        hash_sum = hashlib.sha256()
        for byte_block in iter(lambda: file_obj.file.read(4096), b""):
            hash_sum.update(byte_block)
        file_obj.file.seek(0)
        if hash_sum.hexdigest() != meta['hashSum']:
            raise HTTPException(status_code=400, detail="hashSum does not match sent file")

    @staticmethod
    def _save(file_obj: UploadFile, folder: str) -> None:
        filename = ntpath.basename(file_obj.filename)
        try:
            with open(path.join(folder, filename), 'wb+') as file:
                shutil.copyfileobj(file_obj.file, file)
        except IOError:
            raise HTTPException(status_code=500, detail="File saving failed")
