"""Module defining data submission HTTP API routes."""
import shutil
import requests
import os
from os import path
from collections import namedtuple
from fastapi import FastAPI, BackgroundTasks, File, UploadFile, HTTPException
from . import utils as process_utils


app = FastAPI()
CONFIG = process_utils.read_conf(namedtuple('args', 'config_dir')('./config'))


@app.post("/data/{hashSum}")
async def create_upload_file(background_tasks: BackgroundTasks, hashSum: str,
                             file_submitted: UploadFile = File(...)):
    """Submit raw measurement file to Cloudnet data portal."""
    local_filename = file_submitted.filename
    _save_file_to_disk(file_submitted, local_filename)
    metadata = _read_metadata(hashSum)
    _check_hash(hashSum, local_filename)
    background_tasks.add_task(_process, local_filename, metadata)
    return {"metadata": metadata}


def _save_file_to_disk(file_submitted: str, filename: str) -> None:
    file = open(filename, 'wb+')
    shutil.copyfileobj(file_submitted.file, file)
    file.close()


def _read_metadata(hash_sum: str) -> dict:
    url = path.join(CONFIG['main']['METADATASERVER']['url'], 'metadata', hash_sum)
    session = requests.Session()
    res = session.get(url)
    if str(res.status_code) != '200':
        raise HTTPException(status_code=404, detail="Metadata is not found")
    return res.json()


def _check_hash(hash_sum: str, file_local: str) -> None:
    hash_local = process_utils.sha256sum(file_local)
    if hash_local != hash_sum:
        raise HTTPException(status_code=400,
                            detail="Submitted file incompatible with the given hash")


def _process(filename: str, metadata: dict):
    _move_file_to_correct_folder(metadata, filename)


def _move_file_to_correct_folder(metadata: dict, filename: str) -> None:
    site = metadata['site']['id']
    product = metadata['product']['id']
    yyyy, mm, dd = metadata['measurementDate'].split('-')
    if product == 'radar':
        instrument = 'rpg-fmcw-94'
    elif product == 'lidar':
        instrument = 'chm15k'
    root = CONFIG['main']['PATH']['output']
    folder = path.join(root, site, 'uncalibrated', instrument, yyyy, mm, dd)
    os.makedirs(folder, exist_ok=True)
    shutil.move(filename, folder)
