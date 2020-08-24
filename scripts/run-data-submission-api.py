#!/usr/bin/env python3
import argparse
import ntpath
import uvicorn
from fastapi import FastAPI, File, Form, UploadFile, HTTPException, Depends
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
                             file_submitted: UploadFile = File(...), hashSum: str = Form(...),
                             measurementDate: str = Form(...), instrument: str = Form(...)):
    """Submit file with metadata to Cloudnet data portal."""
    file_submitted.filename = process_utils.add_hash(file_submitted.filename, hashSum)
    meta = {'hashSum': hashSum, 'measurementDate': measurementDate, 'instrument': instrument,
            'filename': ntpath.basename(file_submitted.filename), 'site': credentials.username}
    api.put_metadata(meta)
    api.check_hash(meta, file_submitted)
    api.save_file(meta, file_submitted)
    api.post_hash(meta)
    return {"message": "File submission successful!"}


if __name__ == "__main__":
    server_config = process_utils.read_conf(ARGS)['main']['UVICORN']
    uvicorn.run("run-data-submission-api:app",
                host=server_config['host'],
                port=int(server_config['port']),
                reload=bool(server_config['reload']),
                debug=bool(server_config['debug']),
                workers=int(server_config['workers']))
