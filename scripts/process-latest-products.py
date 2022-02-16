#!/usr/bin/env python3
import subprocess
from datetime import datetime, timedelta
from data_processing import utils
from sys import argv

default_sites = utils.get_cloudnet_sites()
additional_sites = argv[1:]
sites = default_sites + additional_sites

interpreter = 'python3'
script = 'scripts/cloudnet.py'
subcommand = 'process'
wrapper = 'scripts/wrapper.py'
products = 'categorize,classification,iwc,lwc,drizzle'

for site in sites:
    date = datetime.now() - timedelta(3)
    subprocess.check_call([interpreter, wrapper, interpreter, script, '-s', site, '--p', products,
                           '--start', date.strftime('%Y-%m-%d'), subcommand])
