#!/usr/bin/env python3
from operational_processing import utils
import json


def main():
    all_ids = {}
    file_types = ('radar', 'lidar', 'categorize', 'classification', 'iwc', 'lwc', 'drizzle', 'model')
    for file_type in file_types:
        all_ids = {**all_ids, **utils.get_plot_ids(file_type)}
    print(all_ids)
    print(json.dumps(all_ids, indent=1))

if __name__ == "__main__":
    main()
