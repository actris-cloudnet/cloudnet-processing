#!/usr/bin/env python3
import argparse
import os
import shutil
import subprocess
from os import path
import hashlib
import ntpath
import requests
import uvicorn
from fastapi import FastAPI, BackgroundTasks, File, Form, UploadFile, HTTPException, Depends
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
async def create_upload_file(background_tasks: BackgroundTasks,
                             credentials: HTTPBasicCredentials = Depends(security),
                             file_submitted: UploadFile = File(...), hashSum: str = Form(...),
                             measurementDate: str = Form(...), product: str = Form(...)):
    """Submit file with metadata to Cloudnet data portal."""
    meta = {'hashSum': hashSum, 'measurementDate': measurementDate, 'product': product,
            'filename': ntpath.basename(file_submitted.filename), 'site': credentials.username}
    _put_metadata(meta)
    _check_hash(hashSum, file_submitted)
    background_tasks.add_task(_process, file_submitted, meta)
    return {"message": "File submission successful!"}


def _check_hash(reference_hash: str, file_obj: UploadFile) -> None:
    hash_sum = hashlib.sha256()
    for byte_block in iter(lambda: file_obj.file.read(4096), b""):
        hash_sum.update(byte_block)
    file_obj.file.seek(0)
    if hash_sum.hexdigest() != reference_hash:
        raise HTTPException(status_code=400, detail="Unexpected hash in the submitted file")


def _put_metadata(meta: dict) -> None:
    root = _read_conf()['main']['METADATASERVER']['url']
    url = path.join(root, 'metadata', meta['hashSum'])
    res = requests.put(url, json=meta)
    if str(res.status_code) == '200':
        raise HTTPException(status_code=200, detail="File already exists")
    if str(res.status_code) != '201':
        raise HTTPException(status_code=int(res.status_code), detail=res.json())


def _post_hash(hash_sum: str) -> None:
    root = _read_conf()['main']['METADATASERVER']['url']
    url = path.join(root, 'metadata', hash_sum)
    requests.post(url, json={'status': 'uploaded'})


def _process(file_submitted: UploadFile, metadata: dict) -> None:
    config = _read_conf(metadata['site'])
    partial_path, full_path = _create_folder(config, metadata)
    _save_file(file_submitted, full_path)
    _post_hash(metadata['hashSum'])
    if 'chm15k' in partial_path:
        subprocess.check_call(['python3', 'scripts/concat-lidar.py', partial_path])
    start_date = metadata['measurementDate']
    stop_date = process_utils.get_date_from_past(-1, start_date)
    subprocess.check_call(['python3', 'scripts/process-cloudnet.py', metadata['site'],
                           f"--start={start_date}", f"--stop={stop_date}"])
    output_path = path.join(config['main']['PATH']['output'], metadata['site'])
    subprocess.check_call(['python3', 'scripts/plot-quicklooks.py', output_path])


def _create_folder(config: dict, metadata: dict) -> (str, str):
    product = metadata['product']
    instrument = config['site']['INSTRUMENTS'][product]
    root = path.join(config['main']['PATH']['input'], metadata['site'], 'uncalibrated', instrument)
    yyyy, mm, dd = metadata['measurementDate'].split('-')
    full_path = path.join(root, yyyy, mm, dd)
    os.makedirs(full_path, exist_ok=True)
    return root, full_path


def _save_file(file_submitted: UploadFile, folder: str) -> None:
    filename = ntpath.basename(file_submitted.filename)
    file = open(path.join(folder, filename), 'wb+')
    shutil.copyfileobj(file_submitted.file, file)
    file.close()


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
