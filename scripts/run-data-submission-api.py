#!/usr/bin/env python3
import argparse
import os
import shutil
import subprocess
from collections import namedtuple
from os import path
import requests
import uvicorn
from fastapi import FastAPI, BackgroundTasks, File, UploadFile, HTTPException
from data_processing import utils as process_utils

app = FastAPI()

parser = argparse.ArgumentParser(description='Run Cloudnet data submission HTTP API.')
parser.add_argument('--config-dir', type=str, metavar='/FOO/BAR',
                    help='Path to directory containing config files. Default: ./config.',
                    default='./config')
ARGS = parser.parse_args()


@app.post("/data/{hashSum}")
async def create_upload_file(background_tasks: BackgroundTasks, hashSum: str,
                             file_submitted: UploadFile = File(...)):
    """Submit raw measurement file to Cloudnet data portal."""
    local_filename = file_submitted.filename
    _save_file_to_disk(file_submitted, local_filename)
    metadata = _read_metadata(hashSum)
    _check_hash(hashSum, local_filename)
    background_tasks.add_task(_process, local_filename, metadata)
    return {"message": "File submission successful!"}


def _save_file_to_disk(file_submitted: UploadFile, filename: str) -> None:
    file = open(filename, 'wb+')
    shutil.copyfileobj(file_submitted.file, file)
    file.close()


def _read_metadata(hash_sum: str) -> dict:
    config = _read_conf()
    url = path.join(config['main']['METADATASERVER']['url'], 'metadata', hash_sum)
    res = requests.get(url)
    if str(res.status_code) != '200':
        raise HTTPException(status_code=404, detail="Metadata not found")
    return res.json()


def _read_conf(site=None) -> dict:
    if site:
        args = namedtuple('args', 'config_dir site')(ARGS.config_dir, (site, ))
    else:
        args = namedtuple('args', 'config_dir')(ARGS.config_dir)
    return process_utils.read_conf(args)


def _check_hash(hash_sum: str, file_local: str) -> None:
    hash_local = process_utils.sha256sum(file_local)
    if hash_local != hash_sum:
        raise HTTPException(status_code=400,
                            detail="Unexpected hash in the submitted file")


def _process(filename: str, metadata: dict) -> None:
    info = _move_file_to_correct_folder(metadata, filename)
    if 'chm15k' in info['raw_data_path']:
        subprocess.check_call(['python3', 'scripts/concat-lidar.py', info['raw_data_path']])
    stop_date = process_utils.get_date_from_past(-1, info['date'])
    subprocess.check_call(['python3', 'scripts/process-cloudnet.py', metadata['site']['id'],
                           f"--start={info['date']}", f"--stop={stop_date}"])
    output_path = path.join(info['config']['main']['PATH']['output'], metadata['site']['id'])
    subprocess.check_call(['python3', 'scripts/plot-quicklooks.py', output_path])


def _move_file_to_correct_folder(metadata: dict, filename: str) -> dict:
    site = metadata['site']['id']
    product = metadata['product']['id']
    config = _read_conf(site)
    instrument = config['site']['INSTRUMENTS'][product]
    root = path.join(config['main']['PATH']['input'], site, 'uncalibrated', instrument)
    yyyy, mm, dd = metadata['measurementDate'].split('-')
    folder = path.join(root, yyyy, mm, dd)
    os.makedirs(folder, exist_ok=True)
    shutil.move(filename, folder)
    return {'config': config,
            'raw_data_path': root,
            'date': metadata['measurementDate']}


if __name__ == "__main__":
    uvicorn.run("run-data-submission-api:app",
                host='0.0.0.0',
                port=int(5700),
                reload=True,
                debug=False,
                workers=1)
