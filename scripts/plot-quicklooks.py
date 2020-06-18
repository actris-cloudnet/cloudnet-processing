#!/usr/bin/env python3
"""Script for plotting CloudnetPy images."""
import os
from pathlib import Path
import argparse
import netCDF4
from cloudnetpy.plotting import generate_figure
import data_processing.utils as process_utils
from data_processing import metadata_api


def main():
    """The main function."""

    config = process_utils.read_conf(ARGS)
    md_api = metadata_api.MetadataApi(config['main']['METADATASERVER']['url'])

    for file in Path(ARGS.data_path[0]).rglob('*.nc'):
        if not _is_date_in_range(file):
            continue
        nc_file_name = str(file)
        try:
            file_type, uuid = _get_file_info(nc_file_name)
            fields, max_alt = process_utils.get_fields_for_plot(file_type)
        except NotImplementedError as error:
            print(f"Error: {error}")
            continue
        for field in fields:
            image_name = nc_file_name.replace('.nc', f"_{field}.png")
            if _is_plottable(image_name):
                print(f"Plotting: {image_name}")
                try:
                    generate_figure(nc_file_name, [field], show=False,
                                    image_name=image_name, max_y=max_alt,
                                    sub_title=False, title=False, dpi=120)
                    variable_id = process_utils.get_var_id(file_type, field)
                    if not ARGS.no_api:
                        md_api.put_img(image_name, uuid, variable_id)
                except (ValueError, KeyError, AttributeError) as error:
                    print(f"Error: {error}")
                    continue


def _is_date_in_range(path):
    date_in_file = path.name[:8]
    if not date_in_file.isdigit():
        return False
    date_in_file = int(date_in_file)
    start = int(ARGS.start.replace('-', ''))
    stop = int(ARGS.stop.replace('-', ''))
    return start <= date_in_file < stop


def _get_file_info(nc_file_name):
    nc = netCDF4.Dataset(nc_file_name)
    try:
        file_type = nc.cloudnet_file_type
        uuid = nc.file_uuid
    except AttributeError:
        raise NotImplementedError('Missing global attribute')
    nc.close()
    return file_type, uuid


def _is_plottable(image_name):
    if os.path.isfile(image_name) and not ARGS.overwrite:
        return False
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Plot quickloks from Cloudnet data.')
    parser.add_argument('data_path', nargs='+', help='Data path.', metavar='PATH')
    parser.add_argument('--config-dir', type=str, metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')
    parser.add_argument('--start', type=str, metavar='YYYY-MM-DD',
                        help='Starting date. Default is current day - 7.',
                        default=process_utils.get_date_from_past(7))
    parser.add_argument('--stop', type=str, metavar='YYYY-MM-DD',
                        help='Stopping date. Default is the current day.',
                        default=process_utils.get_date_from_past(0))
    parser.add_argument('-o', '--overwrite', dest='overwrite', action='store_true',
                        help='Overwrite existing images', default=False)
    parser.add_argument('-na', '--no-api', dest='no_api', action='store_true',
                        help='Disable API calls. Useful for testing.', default=False)
    ARGS = parser.parse_args()
    main()
