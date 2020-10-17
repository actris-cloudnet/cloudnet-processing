#!/usr/bin/env python3
import argparse
import uvicorn
from fastapi import FastAPI, File, Form, UploadFile, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from data_processing import utils as process_utils
from data_processing.data_submission_api import DataSubmissionApi, ModelDataSubmissionApi, check_hash


app = FastAPI()
security = HTTPBasic()

parser = argparse.ArgumentParser(description='Run Cloudnet data submission HTTP API.')
parser.add_argument('--config-dir', type=str, metavar='/FOO/BAR',
                    help='Path to directory containing config files. Default: ./config.',
                    default='./config')
ARGS = parser.parse_args()

config = process_utils.read_main_conf(ARGS)
api = DataSubmissionApi(config)
model_api = ModelDataSubmissionApi(config)


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
            'filename': file.filename,
            'site': credentials.username}
    url = api.create_url(meta)
    api.put_metadata(url, meta)
    check_hash(hashSum, file)
    api.save_file(meta, file)
    api.update_metadata_status_to_processed(url)
    return {"detail": "File submission successful!"}


@app.post("/modelData/")
async def create_model_upload_file(file: UploadFile = File(...),
                                   modelType: str = Form(...),
                                   hashSum: str = Form(...)):
    """Submit model file."""
    meta = {'hashSum': hashSum,
            'modelType': modelType,
            'filename': file.filename}
    check_hash(hashSum, file)
    model_api.save_file_to_temp(file)
    payload = model_api.create_payload(meta)
    model_api.put_metadata(payload)
    full_path = model_api.move_file_from_temp(payload)
    model_api.link_file(full_path)
    return {"detail": "Model file submission successful!"}


if __name__ == "__main__":
    server_config = process_utils.read_conf(ARGS)['main']['UVICORN']
    uvicorn.run("run-data-submission-api:app",
                host=server_config['host'],
                port=int(server_config['port']),
                reload=bool(server_config['reload']),
                debug=bool(server_config['debug']),
                workers=int(server_config['workers']))
