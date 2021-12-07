#!/usr/bin/env python3
import subprocess
from datetime import datetime, timedelta
from data_processing import utils

sites = utils.get_cloudnet_sites()

interpreter = 'python3'
script = 'scripts/cloudnet.py'
subcommand = 'me'
wrapper = 'scripts/wrapper.py'
products = 'l3-cf,l3-iwc,l3-lwc'

for site in sites:
    date = datetime.now() - timedelta(3)
    date = datetime.strftime(date, '%Y-%m-%d')
    subprocess.check_call([interpreter, wrapper, interpreter, script, '-s', site, '-p', products,
                           '--start', date, subcommand])
