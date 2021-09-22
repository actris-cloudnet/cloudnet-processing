#!/usr/bin/env python3
import requests
import subprocess

url = f"https://cloudnet.fmi.fi/api/sites"
sites = requests.get(url=url).json()

sites = [site['id'] for site in sites if 'cloudnet' in site['type']]

active_campaign_sites = ['mindelo']

sites += active_campaign_sites

wrapper = 'scripts/wrapper.py'
script = 'scripts/process-cloudnet.py'
products = 'radar,lidar,mwr'
interpreter = 'python3'

for site in sites:
    subprocess.check_call([interpreter, wrapper, interpreter, script, site, '--p', products])
