#!/usr/bin/env python3
import subprocess
from data_processing import utils

sites = utils.get_cloudnet_sites()

active_campaign_sites = ['mindelo']

sites += active_campaign_sites

wrapper = 'scripts/wrapper.py'
script = 'scripts/cloudnet.py'
subcommand = 'process'
products = 'radar,lidar,mwr'
interpreter = 'python3'

for site in sites:
    subprocess.check_call([interpreter, wrapper, interpreter, script, '-s', site, '--p', products, subcommand])
