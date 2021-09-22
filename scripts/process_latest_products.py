#!/usr/bin/env python3
import subprocess
from datetime import datetime, timedelta
from data_processing import utils

sites = utils.get_cloudnet_sites()

interpreter = 'python3'
script = 'scripts/process-cloudnet.py'
wrapper = 'scripts/wrapper.py'
products = 'categorize,classification,iwc,lwc,drizzle'

for site in sites:
    date = datetime.now() - timedelta(3)
    date = datetime.strftime(date, '%Y-%m-%d')
    subprocess.check_call([interpreter, wrapper, interpreter, script, site, '--p', products,
                           '--start', date])
