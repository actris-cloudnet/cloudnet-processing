#!/usr/bin/env python3
import subprocess
import logging
from cloudnet_processing import utils

MINIMUM_SIZE = 20200

uploaded_metadata = utils.get_from_data_portal_api('upload-model-metadata', {'status': 'uploaded'})

valid_metadata = [data for data in uploaded_metadata if int(data['size']) > MINIMUM_SIZE]
all_sites = [row['site']['id'] for row in valid_metadata]
sites = list(set(all_sites))

interpreter = 'python3'
wrapper = 'scripts/wrapper.py'
script = 'scripts/cloudnet.py'
subcommand = 'model'
subcommand_freeze = 'freeze'

if not sites:
    logging.info('No unprocessed model data')
else:

    for site in sites:
        subprocess.check_call([interpreter, wrapper, interpreter, script, '-s', site, subcommand])

    subprocess.check_call([interpreter, wrapper, interpreter, script, '-s', 'all', subcommand_freeze])
