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

    def put_metadata(self, meta: dict) -> None:
        url = path.join(self._meta_server, 'metadata', meta['hashSum'])
        res = self._session.put(url, json=meta)
        if str(res.status_code) == '200':
            raise HTTPException(status_code=200, detail="File already exists")
        if str(res.status_code) != '201':
            raise HTTPException(status_code=int(res.status_code), detail=res.json())

    def post_hash(self, meta: dict) -> None:
        url = path.join(self._meta_server, 'metadata', meta['hashSum'])
        self._session.post(url, json={'status': 'uploaded'})

    def save_file(self, meta: dict, file_obj: UploadFile) -> None:
        folder = self._create_folder(meta)
        self._save(file_obj, folder)

    def _create_folder(self, meta: dict) -> (str, str):
        root = path.join(self.config['PATH']['api_files'], meta['site'], meta['instrument'])
        yyyy, mm, dd = meta['measurementDate'].split('-')
        full_path = path.join(root, yyyy, mm, dd)
        os.makedirs(full_path, exist_ok=True)
        return full_path

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
