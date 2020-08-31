# ACTRIS Cloudnet data-processing
![](https://github.com/actris-cloudnet/data-processing/workflows/Cloudnet%20processing%20CI/badge.svg)

Various scripts used in Cloudnet data transfer and processing.

### Installation
```
$ git clone git@github.com:actris-cloudnet/data-processing.git
$ cd data-processing
$ python3 -m venv venv
$ source venv/bin/activate
(venv) $ pip3 install --upgrade pip
(venv) $ pip3 install .
```

### Scripts

The scripts are located in `scripts/` folder and should be run from the root: 
```
$ scripts/<script_name.py> arg1 --arg2=foo ...
```
The following scripts are provided:

### `run-data-submission-api.py`

Run HTTP API for data submission to Cloudnet data portal.

```
usage: run-data-submission-api.py [-h] [--config-dir /FOO/BAR]
```

Optional arguments:

| Short | Long            | Default           | Description         | 
| :---  | :---            | :---              | :---                |
| `-h`  | `-help`         |                   | Show help and exit. |
|       | `--config-dir`  | `./config`        | Path to directory containing config files. |


See [API reference](https://github.com/actris-cloudnet/dataportal/blob/master/docs/data-upload.md) on how to use the 
data submission API from the client's side.

### `fix-model-files.py`

Fix Cloudnet model files.

```
usage: fix-model-files.py [-h] [--model-type MODEL] [--config-dir /FOO/BAR]
                          [--input /FOO/BAR] [--output /FOO/BAR] [-d] [-na]
                          SITE [SITE ...]
```

Positional arguments:

| Name   | Description | 
| :---   | :---        |
| `site` |  Site name. |

Optional arguments:

| Short | Long            | Default           | Description         | 
| :---  | :---            | :---              | :---                |
| `-h`  | `-help`         |                   | Show help and exit. |
|       | `--model-type`  | `ecmwf`           |  Model type name.   |
|       | `--config-dir`  | `./config`        | Path to directory containing config files. |
|       | `--input`       | from `config/main.ini`     | Input directory path.|
|       | `--output`      | from `config/main.ini`    | Output directory path. |
| `-d`  | `--dry`         | `False`             | Try the script without writing any files or calling API. |
| `-na`  | `--no-api`     | `False`             | Disable API calls. Useful for testing. |

### `concat-lidar.py`
Concatenate CHM15k lidar files (several files per day) into daily files.

```
usage: concat-lidar.py [-h] [--output /FOO/BAR/] [-o] [--year YEAR]
                       [--month {1,2,3,4,5,6,7,8,9,10,11,12}]
                       [--day {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31}]
                       [-l N]
                       PATH [PATH ...]
```

Positional arguments:

| Name   | Description | 
| :---   | :---        |
| `path` |  Path to files.|

Optional arguments:

| Short | Long            | Default           | Description                                | 
| :---  | :---            | :---              | :---                                       |
| `-h`  | `-help`         |                   | Show help and exit. |
|       | `--output`      | same as the positional argument `path` | Output directory. |
| `-o`  | `--overwrite`   | `False`             | Overwrite data in existing files. |
|       | `--year`   |              | Limit to certain year only. |
|       | `--month`   |              | Limit to certain month only. |
|       | `--day`   |                | Limit to certain day only. |
| `-l`  | `--limit`   |              | Run only on folders modified within N hours. |

Examples:
```
$ scripts/concat-lidar.py /data/bucharest/uncalibrated/chm15k/ 
```
This will concatenate, for example, `/data/bucharest/uncalibrated/chm15k/2020/01/15/*.nc` into 
`/data/bucharest/uncalibrated/chm15k/2020/chm15k_20200115.nc`, and so on.

After the initial concatenation for all existing folders has been performed, 
it is usually sufficient to use the `-l` switch:
```
$ scripts/concat-lidar.py /data/bucharest/uncalibrated/chm15k/ -l=24
```
Which finds the folders updated within 24 hours and overrides daily files from these folders 
making sure they are always up to date (if the script is run daily).

### `process-cloudnet.py`
Create Cloudnet products.

Prerequisites:
* Fix the `input` and `output` data paths in `config/main.ini` 
* Make sure that the instrument list is correct in `config/<site>.ini`.
* Make sure you have `output/<site>/calibrated/<model>/<year>` folder containing pre-processed model data. 
* If you use CHM15k lidar, make sure there are pre-processed daily files in `input/uncalibrated/chm15k/<year>` folder.

```
usage: process-cloudnet.py [-h] [--config-dir /FOO/BAR] [--start YYYY-MM-DD]
                           [--stop YYYY-MM-DD] [--input /FOO/BAR]
                           [--output /FOO/BAR] [-o] [-k] [-na]
                           SITE [SITE ...]
```

Positional arguments:

| Name   | Description | 
| :---   | :---        |
| `site` |  Site name.|

Optional arguments:

| Short | Long            | Default           | Description                                | 
| :---  | :---            | :---              | :---                                       |
| `-h`  | `-help`         |                   | Show help and exit. |
|       | `--config-dir`  | `./config`        | Path to directory containing config files. |
|       | `--start`       | `current day - 7` | Starting date. |
|       | `--stop`        | `current day - 1 `| Stopping date. |
|       | `--input`       | from `config/main.ini` | Input folder path. |
|       | `--output`      | from `config/main.ini` | Output folder path. |
| `-o`  | `--overwrite`   | `False`             | Overwrite data in existing files. |
| `-k`  | `--keep_uuid`   | `True`              | Keep ID of old file even if the data is overwritten. |
| `-na`  | `--no-api`     | `False`             | Disable API calls. Useful for testing. |


### `plot-quicklooks.py`
Plot quicklooks from Cloudnet data.

```
usage: plot-quicklooks.py [-h] [--config-dir /FOO/BAR] [--start YYYY-MM-DD]
                          [--stop YYYY-MM-DD] [-o] [-na]
                          PATH [PATH ...]
```

Positional arguments:

| Name   | Description | 
| :---   | :---        |
| `path` |  Path to files.|

Optional arguments:

| Short | Long            | Default          | Description                                | 
| :---  | :---            | :---             | :---                                       |
| `-h`  | `-help`         |                  | Show help and exit.                        |
|       | `--config-dir`  | `./config`       | Path to directory containing config files. |
|       | `--start`       | `current day -7` | Starting date. |
|       | `--stop`        | `current day`    | Stopping date. |
| `-o`  | `--overwrite`   | `False`            | Overwrite existing images        |
| `-na`  | `--no-api`     | `False`            | Disable API calls. Useful for testing.     |

### `put-missing-files.py`
Put missing files to database.

```
usage: put-missing-files.py [-h] [--config-dir /FOO/BAR] PATH [PATH ...]
```

Positional arguments:

| Name   | Description | 
| :---   | :---        |
| `path` |  Path to files to be checked recursively.|


Optional arguments:

| Short | Long             | Default      | Description                                | 
| :---  | :---             | :---         | :---                                       |
| `-h`  | `-help`          |              | Show help and exit.                        |
|       | `--config-dir`   | `./config`   | Path to directory containing config files. |


### `freeze.py`
Freeze selected files.

```
usage: freeze.py [-h] [--config-dir /FOO/BAR] [-na]
```

Optional arguments:

| Short | Long             | Default      | Description                                | 
| :---  | :---             | :---         | :---                                       |
| `-h`  | `-help`          |              | Show help and exit.                        |
|       | `--config-dir`   | `./config`   | Path to directory containing config files. |


### `map-variable-names.py`
Print list of Cloudnet variables.

```
usage: map-variable-names.py
```

### Tests
Run unit tests
```
$ pytest
```

Run end-to-end tests:
```
$ for f in tests/e2e/*/main.py; do $f; done
```

### Licence
MIT

