#!venv/bin/python3
"""Master script for CloudnetPy processing."""
import argparse
import configparser
import yaml


def main():
    site_name = ARGS.site[0]

    main_config = _read_config('main')
    site_config = _read_config(site_name)

    site_data = _read_site_info(main_config, site_name)


def _read_config(name):
    config = configparser.ConfigParser()
    config.read(f"config/{name}.ini")
    return config


def _read_site_info(config, site_name):
    site_file = f"{config['PATH']['resources']}Site.yml"
    with open(site_file, 'r') as stream:
        data = yaml.safe_load(stream)
    for _, item in data['items'].items():
        if item['id'] == site_name:
            return item


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Cloudnet data.')
    parser.add_argument('site', nargs='+', help='Site Name', choices=['bucharest'])
    ARGS = parser.parse_args()
    main()
