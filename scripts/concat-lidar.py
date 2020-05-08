#!python3
"""Script for concatenating individual chm15k-files into daily files."""
import os
import time
import argparse
import numpy as np
import netCDF4
from tqdm import tqdm

lib = __import__('operational-processing').concat_lib

CONSTANTS = ['range', 'wavelength', 'scaling', 'zenith']
VARIABLES = ['time', 'beta_raw', 'stddev', 'nn1', 'nn2', 'nn3']


def main():

    input_dir = ARGS.input[0]
    output_dir = ARGS.output or input_dir

    year_range = _get_range('year')
    years = lib.get_dirs_in_range(input_dir, year_range)

    for year in years:
        month_range = _get_range('month')
        months = lib.get_dirs_in_range('/'.join((input_dir, year)), month_range)
        for month in months:
            day_range = _get_range('day')
            days = lib.get_dirs_in_range('/'.join((input_dir, year, month)), day_range)
            fun = _print_info(days, year, month)
            for day in fun(days):
                date = (year, month, day)
                full_input_dir = lib.get_full_input_path(input_dir, date)
                _concat(full_input_dir, output_dir, date)
    print('..done')


def _print_info(days, year, month):
    if ARGS.limit:
        print('Finding active folders..')

        def fun(x):
            return x
    else:
        fun = tqdm
    if len(days) > 0 and not ARGS.limit:
        print(f"Concatenating year {year}, month {month}")
    return fun


def _get_range(period):
    arg = getattr(ARGS, period, None)
    if arg:
        return [arg, arg]
    return lib.get_default_range(period)


def _concat(full_input_dir, output_dir, date):

    file_new_name = _prepare_output_path(output_dir, date)

    if os.path.isfile(file_new_name) and not ARGS.overwrite and not ARGS.limit:
        return

    if ARGS.limit:
        if not _is_active_folder(full_input_dir):
            return
        print(f"Concatenating from active folder: {full_input_dir}")

    file_new = netCDF4.Dataset(file_new_name, 'w', format='NETCDF4_CLASSIC')

    files = lib.get_list_of_nc_files(full_input_dir)
    files = lib.remove_files_with_wrong_date(files, date)

    first_file_of_day = netCDF4.Dataset(files[0])

    _create_dimensions(file_new, first_file_of_day)
    _create_global_attributes(file_new, first_file_of_day)
    _write_initial_data(file_new, first_file_of_day)

    if len(files) > 1:
        for file in files[1:]:
            _append_data(file_new, netCDF4.Dataset(file))

    file_new.close()


def _is_active_folder(input_dir):
    last_modified = time.time() - os.path.getmtime(input_dir)
    return (last_modified / 3600) < ARGS.limit


def _prepare_output_path(output_dir, date):
    the_dir = '/'.join((output_dir, date[0]))
    os.makedirs(the_dir, exist_ok=True)
    yyyymmdd = f"{date[0]}{date[1].zfill(2)}{date[2].zfill(2)}"
    return '/'.join((the_dir, f"chm15k_{yyyymmdd}.nc"))


def _create_dimensions(file_new, file_source):
    n_range = len(file_source['range'])
    file_new.createDimension('time', None)
    file_new.createDimension('range', n_range)


def _create_global_attributes(file_new, file_source):
    file_new.Conventions = 'CF-1.7'
    _copy_attributes(file_source, file_new)


def _copy_attributes(source, target):
    for attr in source.ncattrs():
        value = getattr(source, attr)
        setattr(target, attr, value)


def _write_initial_data(file_new, file_source):
    for key in CONSTANTS + VARIABLES:
        array = file_source[key][:]
        var = file_new.createVariable(key, lib.get_dtype(key, array), lib.get_dim(file_new, array),
                                      zlib=True, complevel=3, shuffle=False)
        var[:] = array
        _copy_attributes(file_source[key], var)


def _append_data(file_base, file):
    ind0 = len(file_base.variables['time'])
    ind1 = ind0 + len(file.variables['time'])
    for key in VARIABLES:
        array = file[key][:]
        if array.ndim == 1:
            file_base.variables[key][ind0:ind1] = array
        else:
            file_base.variables[key][ind0:ind1, :] = array


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Concatenate multiple CHM15k files into daily files.')
    parser.add_argument('input', metavar='/path/to/data/', nargs='+', help='Input directory')
    parser.add_argument('--output', metavar='/path/to/', help='Output directory')
    parser.add_argument('-o', '--overwrite', dest='overwrite', action='store_true',
                        help='Overwrites any existing daily files', default=False)
    parser.add_argument('--year', type=int, help='Limit to certain year only.')
    parser.add_argument('--month', type=int, choices=np.arange(1, 13), help='Limit to certain month only.')
    parser.add_argument('--day', type=int, choices=np.arange(1, 32), help='Limit to certain day only.')
    parser.add_argument('-l', '--limit', metavar='N', type=int, help='Run only on folders modified within N hours.')
    ARGS = parser.parse_args()
    main()
