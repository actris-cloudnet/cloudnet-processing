import requests


def read_site_info(site_name):
    url = f"https://altocumulus.fmi.fi/api/sites/"
    sites = requests.get(url=url, verify=False).json()
    for site in sites:
        if site['id'] == site_name:
            return site
