#!python3
"""Master script for CloudnetPy processing."""
import time
import argparse
import configparser
import yaml
import importlib
from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import FileSystemEventHandler
process_utils = importlib.import_module("operational-processing").utils

lib = __import__('operational-processing').concat_lib


def main():
    site_name = ARGS.site[0]

    conf = process_utils.read_conf()

    site_info = _read_site_info(conf, site_name)

    paths_to_listen = _build_paths_to_listen(conf, site_name)

    print('Listening to:')
    for path in paths_to_listen:
        print(path)

    observers = [_add_watcher(path) for path in paths_to_listen]

    try:
        while True:
            time.sleep(2)
    except KeyboardInterrupt:
        for obs in observers:
            obs.stop()
    for obs in observers:
        obs.join()


def _read_site_info(conf, site_name):
    site_file = f"{conf['main']['PATH']['resources']}Site.yml"
    with open(site_file, 'r') as stream:
        data = yaml.safe_load(stream)
    for item in data['items'].values():
        if item['id'] == site_name:
            return item


def _build_paths_to_listen(conf, site_name):
    def _get_root(key, file_type):
        target = 'uncalibrated' if key == 'input' else 'calibrated'
        path = conf['main']['PATH'][key]
        return '/'.join((path, site_name, target, instruments[file_type]))

    instruments = conf['site']['INSTRUMENTS']
    return (_get_root('input', 'lidar'),
            _get_root('input', 'radar'),
            _get_root('output', 'model'))


def _add_watcher(path):
    observer = Observer()
    observer.schedule(_Sniffer(path), path=path, recursive=True)
    observer.start()
    return observer


class _Sniffer(FileSystemEventHandler):
    def __init__(self, path):
        self.path = path
        self._last_target = None
        self._timestamp = time.time()

    def on_any_event(self, event):

        if event.is_directory or event.event_type == 'deleted':
            return

        if self._is_fake_event(event):
            self._timestamp = time.time()
            return

        self._last_target = event.src_path
        self._timestamp = time.time()
        date = lib.find_date(event.src_path)
        print(date)


    def _is_fake_event(self, event):
        time_difference = time.time() - self._timestamp
        return time_difference < 3 and self._last_target == event.src_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Cloudnet data.')
    parser.add_argument('site', nargs='+', help='Site Name', choices=['bucharest'])
    ARGS = parser.parse_args()
    main()
