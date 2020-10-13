#!/usr/bin/env python3
import argparse
import ntpath
import uvicorn
from fastapi import FastAPI, File, Form, UploadFile, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from data_processing import utils as process_utils
from data_processing.data_submission_api import DataSubmissionApi

app = FastAPI()
security = HTTPBasic()

parser = argparse.ArgumentParser(description='Run Cloudnet data submission HTTP API.')
parser.add_argument('--config-dir', type=str, metavar='/FOO/BAR',
                    help='Path to directory containing config files. Default: ./config.',
                    default='./config')
ARGS = parser.parse_args()

config = process_utils.read_main_conf(ARGS)
api = DataSubmissionApi(config)


@app.post("/data/")
async def create_upload_file(credentials: HTTPBasicCredentials = Depends(security),
                             file: UploadFile = File(...),
                             hashSum: str = Form(...),
                             measurementDate: str = Form(...),
                             instrument: str = Form(...)):
    """Submit file with metadata to Cloudnet data portal."""
    file.filename = process_utils.add_hash_to_filename(file.filename, hashSum)
    meta = {'hashSum': hashSum,
            'measurementDate': measurementDate,
            'instrument': instrument,
            'filename': ntpath.basename(file.filename),
            'site': credentials.username}
    api.put_metadata(meta)
    api.check_hash(meta, file)
    api.save_file(meta, file)
    api.update_metadata_status_to_processed(meta)
    return {"detail": "File submission successful!"}


@app.put("/modelData/")
async def create_model_upload_file(file: UploadFile = File(...),
                                   site: str = Form(...),
                                   date: str = Form(...),
                                   modelType: str = Form(...),
                                   hashSum: str = Form(...)):
    """Submit model file."""
    meta = {'hashSum': hashSum,
            'date': date,
            'modelType': modelType,
            'filename': ntpath.basename(file.filename),
            'site': site}
    api.check_hash(meta, file)
    api.put_model_metadata(meta, file)
    api.save_file(meta, file, model_file=True)
    return {"detail": "Model file submission successful!"}


if __name__ == "__main__":
    server_config = process_utils.read_conf(ARGS)['main']['UVICORN']
    uvicorn.run("run-data-submission-api:app",
                host=server_config['host'],
                port=int(server_config['port']),
                reload=bool(server_config['reload']),
                debug=bool(server_config['debug']),
                workers=int(server_config['workers']))
