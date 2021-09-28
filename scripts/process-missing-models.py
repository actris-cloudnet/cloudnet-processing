#!/usr/bin/env python3
import subprocess
import logging
from data_processing import utils

MINIMUM_SIZE = 20200

uploaded_metadata = utils.get_from_data_portal_api('upload-model-metadata', {'status': 'uploaded'})

valid_metadata = [data for data in uploaded_metadata if int(data['size']) > MINIMUM_SIZE]
all_sites = [row['site']['id'] for row in valid_metadata]
sites = list(set(all_sites))

interpreter = 'python3'
wrapper = 'scripts/wrapper.py'
script = 'scripts/process-model.py'
script_freeze = 'scripts/freeze.py'

if not sites:
    logging.info('No unprocessed model data')
else:

    for site in sites:
        subprocess.check_call([interpreter, wrapper, interpreter, script, site])

    subprocess.check_call([interpreter, wrapper, interpreter, script_freeze])
