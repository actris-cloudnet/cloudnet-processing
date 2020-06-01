#!/usr/bin/env python3
"""Script for plotting CloudnetPy images."""
import os
from pathlib import Path
import argparse
from cloudnetpy.plotting import generate_figure
from cloudnetpy.plotting.plot_meta import ATTRIBUTES
import netCDF4
from operational_processing import utils, metadata_api


def main():

    config = utils.read_conf(ARGS)
    md_api = metadata_api.MetadataApi(config['main']['METADATASERVER']['url'])

    for file in Path(ARGS.data_path[0]).rglob('*.nc'):
        if not _is_date_in_range(file):
            continue
        nc_file_name = str(file)
        try:
            file_type, uuid = _get_file_info(nc_file_name)
            fields, max_alt = _get_fields_for_plot(file_type)
        except NotImplementedError as error:
            print(error)
            continue
        for field in fields:
            image_name = nc_file_name.replace('.nc', f"_{field}.png")
            if _is_plottable(image_name):
                print(f"Plotting: {image_name}")
                try:
                    generate_figure(nc_file_name, [field], show=False,
                                    image_name=image_name, max_y=max_alt,
                                    sub_title=False, dpi=150)
                    variable_info = _get_variable_info(file_type, field)
                    print(file_type, image_name, uuid, variable_info)
                    md_api.put_img(image_name, uuid, variable_info)
                except (ValueError, KeyError, AttributeError) as error:
                    print(f"Error: {error}")
                    continue


def _get_variable_info(cloudnet_file_type, field):
    return {
        'human_readable_name': ATTRIBUTES[field].name,
        'id': f"{cloudnet_file_type}-{field}"
    }


def _is_date_in_range(path):
    date_in_file = path.name[:8]
    if not date_in_file.isdigit():
        return False
    date_in_file = int(date_in_file)
    start = int(ARGS.start.replace('-', ''))
    stop = int(ARGS.stop.replace('-', ''))
    return start <= date_in_file < stop


def _get_file_info(nc_file_name):
    attr_names = ['cloudnet_file_type', 'file_uuid']
    nc = netCDF4.Dataset(nc_file_name)
    for name in attr_names:
        if not hasattr(nc, name):
            raise NotImplementedError
    file_type, uuid = [getattr(nc, name) for name in attr_names]
    nc.close()
    return file_type, uuid


def _get_fields_for_plot(cloudnet_file_type):
    max_alt = 10
    if cloudnet_file_type == 'categorize':
        fields = ['v', 'width', 'ldr', 'Z', 'beta', 'lwp', 'Tw', 'radar_gas_atten', 'radar_liquid_atten', 'v_sigma']
    elif cloudnet_file_type == 'classification':
        fields = ['target_classification', 'detection_status']
    elif cloudnet_file_type == 'iwc':
        fields = ['iwc', 'iwc_retrieval_status', 'iwc_error']
    elif cloudnet_file_type == 'lwc':
        fields = ['lwc', 'lwc_retrieval_status', 'lwc_error']
        max_alt = 6
    elif cloudnet_file_type == 'model':
        fields = ['cloud_fraction', 'uwind', 'vwind', 'temperature', 'q', 'pressure']
    elif cloudnet_file_type == 'lidar':
        fields = ['beta', 'beta_raw']
    elif cloudnet_file_type == 'radar':
        fields = ['Ze', 'v', 'width', 'ldr']
    elif cloudnet_file_type == 'drizzle':
        fields = ['Do', 'mu', 'S', 'drizzle_N', 'drizzle_lwc', 'drizzle_lwf', 'v_drizzle', 'v_air']
        max_alt = 4
    else:
        raise NotImplementedError
    return fields, max_alt


def _is_plottable(image_name):
    if os.path.isfile(image_name) and not ARGS.overwrite:
        return False
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Plot quickloks from Cloudnet data.')
    parser.add_argument('data_path', nargs='+', help='Data path.')
    parser.add_argument('--config-dir', type=str, metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.', default='./config')
    parser.add_argument('--start', type=str, metavar='YYYY-MM-DD', help='Starting date. Default is current day - 7.',
                        default=utils.get_date_from_past(7))
    parser.add_argument('--stop', type=str, metavar='YYYY-MM-DD', help='Stopping date. Default is the current day.',
                        default=utils.get_date_from_past(0))
    parser.add_argument('-o', '--overwrite', dest='overwrite', action='store_true',
                        help='Overwrites existing images', default=False)
    ARGS = parser.parse_args()
    main()
