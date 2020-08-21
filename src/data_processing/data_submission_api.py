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

    def __init__(self, configs):
        self.configs = configs

    def put_metadata(self, meta: dict) -> None:
        site = meta['site']
        root = self.configs[site]['main']['METADATASERVER']['url']
        url = path.join(root, 'metadata', meta['hashSum'])
        res = requests.put(url, json=meta)
        if str(res.status_code) == '200':
            raise HTTPException(status_code=200, detail="File already exists")
        if str(res.status_code) != '201':
            raise HTTPException(status_code=int(res.status_code), detail=res.json())

    @staticmethod
    def check_hash(meta: dict, file_obj: UploadFile) -> None:
        hash_sum = hashlib.sha256()
        for byte_block in iter(lambda: file_obj.file.read(4096), b""):
            hash_sum.update(byte_block)
        file_obj.file.seek(0)
        if hash_sum.hexdigest() != meta['hashSum']:
            raise HTTPException(status_code=400, detail="hashSum does not match sent file")

    def post_hash(self, meta: dict) -> None:
        site = meta['site']
        root = self.configs[site]['main']['METADATASERVER']['url']
        url = path.join(root, 'metadata', meta['hashSum'])
        requests.post(url, json={'status': 'uploaded'})

    def save_file(self, meta: dict, file_obj: UploadFile) -> None:
        folder = self._create_folder(meta)
        self._save(file_obj, folder)

    def _create_folder(self, meta: dict) -> (str, str):
        site = meta['site']
        product = meta['product']
        instrument = self.configs[site]['site']['INSTRUMENTS'][product]
        root = path.join(self.configs[site]['main']['PATH']['input'], site, 'uncalibrated', instrument)
        yyyy, mm, dd = meta['measurementDate'].split('-')
        full_path = path.join(root, yyyy, mm, dd)
        os.makedirs(full_path, exist_ok=True)
        return full_path

    @staticmethod
    def _save(file_obj: UploadFile, folder: str) -> None:
        filename = ntpath.basename(file_obj.filename)
        try:
            with open(path.join(folder, filename), 'wb+') as file:
                shutil.copyfileobj(file_obj.file, file)
        except IOError:
            raise HTTPException(status_code=500, detail="File saving failed")
