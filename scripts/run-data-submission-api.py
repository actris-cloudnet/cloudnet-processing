#!/usr/bin/env python3
import argparse
import os
import shutil
from os import path
import hashlib
import ntpath
import requests
import uvicorn
from fastapi import FastAPI, File, Form, UploadFile, HTTPException, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from data_processing import utils as process_utils

app = FastAPI()
security = HTTPBasic()

parser = argparse.ArgumentParser(description='Run Cloudnet data submission HTTP API.')
parser.add_argument('--config-dir', type=str, metavar='/FOO/BAR',
                    help='Path to directory containing config files. Default: ./config.',
                    default='./config')
ARGS = parser.parse_args()


@app.post("/data/")
async def create_upload_file(credentials: HTTPBasicCredentials = Depends(security),
                             file_submitted: UploadFile = File(...), hashSum: str = Form(...),
                             measurementDate: str = Form(...), product: str = Form(...)):
    """Submit file with metadata to Cloudnet data portal."""
    file_submitted.filename = process_utils.add_hash(file_submitted.filename, hashSum)
    meta = {'hashSum': hashSum, 'measurementDate': measurementDate, 'product': product,
            'filename': ntpath.basename(file_submitted.filename), 'site': credentials.username}
    config = _read_conf(meta['site'])
    _put_metadata(meta, config)
    _check_hash(hashSum, file_submitted)
    _save_file_to_correct_folder(meta, file_submitted, config)
    _post_hash(config, meta['hashSum'])
    return {"message": "File submission successful!"}


def _put_metadata(meta: dict, config: dict) -> None:
    root = config['main']['METADATASERVER']['url']
    url = path.join(root, 'metadata', meta['hashSum'])
    res = requests.put(url, json=meta)
    if str(res.status_code) == '200':
        raise HTTPException(status_code=200, detail="File already exists")
    if str(res.status_code) != '201':
        raise HTTPException(status_code=int(res.status_code), detail=res.json())


def _check_hash(reference_hash: str, file_obj: UploadFile) -> None:
    hash_sum = hashlib.sha256()
    for byte_block in iter(lambda: file_obj.file.read(4096), b""):
        hash_sum.update(byte_block)
    file_obj.file.seek(0)
    if hash_sum.hexdigest() != reference_hash:
        raise HTTPException(status_code=400, detail="Unexpected hash in the submitted file")


def _save_file_to_correct_folder(meta: dict, file_obj: UploadFile, config: dict) -> None:
    folder = _create_folder(config, meta)
    _save_file(file_obj, folder)


def _create_folder(config: dict, meta: dict) -> (str, str):
    product = meta['product']
    instrument = config['site']['INSTRUMENTS'][product]
    root = path.join(config['main']['PATH']['input'], meta['site'], 'uncalibrated', instrument)
    yyyy, mm, dd = meta['measurementDate'].split('-')
    full_path = path.join(root, yyyy, mm, dd)
    os.makedirs(full_path, exist_ok=True)
    return full_path


def _save_file(file_obj: UploadFile, folder: str) -> None:
    filename = ntpath.basename(file_obj.filename)
    try:
        with open(path.join(folder, filename), 'wb+') as file:
            shutil.copyfileobj(file_obj.file, file)
    except IOError:
        raise HTTPException(status_code=500, detail="File saving failed.")


def _post_hash(config: dict, hash_sum: str) -> None:
    root = config['main']['METADATASERVER']['url']
    url = path.join(root, 'metadata', hash_sum)
    requests.post(url, json={'status': 'uploaded'})


def _read_conf(site=None) -> dict:
    if site:
        ARGS.site = (site, )
    return process_utils.read_conf(ARGS)


if __name__ == "__main__":
    server_config = _read_conf()['main']['UVICORN']
    uvicorn.run("run-data-submission-api:app",
                host=server_config['host'],
                port=int(server_config['port']),
                reload=bool(server_config['reload']),
                debug=bool(server_config['debug']),
                workers=int(server_config['workers']))
