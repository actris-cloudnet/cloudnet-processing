#!venv/bin/python3
"""Master script for CloudnetPy processing."""
import time
import argparse
import configparser
import yaml
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

lib = __import__('operational-processing').concat_lib


def main():
    site_name = ARGS.site[0]

    conf = {'main': _read_config('main'),
            'site': _read_config(site_name)}

    site_data = _read_site_info(conf, site_name)

    paths_to_listen = _build_paths_to_listen(conf, site_name)
    observers = [_add_watcher(path) for path in paths_to_listen]

    try:
        while True:
            time.sleep(2)
    except KeyboardInterrupt:
        for obs in observers:
            obs.stop()
    for obs in observers:
        obs.join()


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


def _build_paths_to_listen(conf, site_name):
    def _get_root(key):
        target = 'uncalibrated' if key == 'input' else 'calibrated'
        return '/'.join((conf['main']['PATH'][key], site_name, target))

    instruments = conf['site']['INSTRUMENTS']
    lidar_path = '/'.join((_get_root('input'), instruments['lidar']))
    radar_path = '/'.join((_get_root('input'), instruments['radar']))
    model_path = '/'.join((_get_root('output'), instruments['model']))
    return lidar_path, radar_path, model_path


def _add_watcher(path):
    observer = Observer()
    observer.schedule(_Sniffer(path), path=path, recursive=True)
    observer.start()
    return observer


class _Sniffer(FileSystemEventHandler):
    def __init__(self, path):
        self.path = path
        self._timestamp = time.time()

    def on_modified(self, event):
        """Actually seems to track both NEW and MODIFIED files."""
        if event.is_directory:
            return
        # Hack to prevent watchdog bug that spawns multiple events.
        # See: https://github.com/gorakhargosh/watchdog/issues/93
        time.sleep(1)
        if (time.time() - self._timestamp) > 1.5:
            date = lib.find_date(event.src_path)
        self._timestamp = time.time()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Cloudnet data.')
    parser.add_argument('site', nargs='+', help='Site Name', choices=['bucharest'])
    ARGS = parser.parse_args()
    main()
