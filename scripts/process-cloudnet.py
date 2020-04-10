#!venv/bin/python3
"""Master script for CloudnetPy processing."""
import argparse
import configparser
import yaml
import importlib

lib = importlib.import_module("operational-processing").concat_lib
utils = importlib.import_module("operational-processing").utils

def main():
    site_name = ARGS.site[0]

    conf = {'main': _read_config('main'),
            'site': _read_config(site_name)}

    site_info = utils.read_site_info(site_name)
    data_paths = _get_input_data_paths(conf, site_name)




def _read_config(conf_type):
    config = configparser.ConfigParser()
    config.read(f"config/{conf_type}.ini")
    return config


def _read_site_info(conf, site_name):
    site_file = f"{conf['main']['PATH']['resources']}Site.yml"
    with open(site_file, 'r') as stream:
        data = yaml.safe_load(stream)
    for item in data['items'].values():
        if item['id'] == site_name:
            return item


def _get_input_data_paths(conf, site_name):
    def _get_root(key, file_type):
        target = 'uncalibrated' if key == 'input' else 'calibrated'
        path = conf['main']['PATH'][key]
        return '/'.join((path, site_name, target, instruments[file_type]))

    instruments = conf['site']['INSTRUMENTS']
    return (_get_root('input', 'lidar'),
            _get_root('input', 'radar'),
            _get_root('output', 'model'))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Cloudnet data.')
    parser.add_argument('site', nargs='+', help='Site Name', choices=['bucharest'])
    ARGS = parser.parse_args()
    main()
