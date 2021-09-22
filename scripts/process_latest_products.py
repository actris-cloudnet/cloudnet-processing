#!/usr/bin/env python3
import requests
import subprocess
from datetime import datetime, timedelta

url = f"https://cloudnet.fmi.fi/api/sites"
sites = requests.get(url=url).json()

sites = [site['id'] for site in sites if 'cloudnet' in site['type']]

interpreter = 'python3'
script = 'scripts/process-cloudnet.py'
wrapper = 'scripts/wrapper.py'
products = 'categorize,classification,iwc,lwc,drizzle'

for site in sites:
    date = datetime.now() - timedelta(3)
    date = datetime.strftime(date, '%Y-%m-%d')
    subprocess.check_call([interpreter, wrapper, interpreter, script, site, '--p', products,
                           '--start', date])
