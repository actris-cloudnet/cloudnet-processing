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


### `process-cloudnet.py`
Create Cloudnet products.

Prerequisites:
* Fix the `input` and `output` data paths in `config/main.ini` 
* Make sure that the instrument list is correct in `config/<site>.ini`.
* Make sure you have `output/<site>/calibrated/<model>/<year>` folder containing pre-processed model data. 
* If you use CHM15k lidar, make sure there are pre-processed daily files in `input/uncalibrated/chm15k/<year>` folder.

```
usage: process-cloudnet.py [-h] [--config-dir /FOO/BAR] [--start YYYY-MM-DD]
                           [--stop YYYY-MM-DD] [--input /FOO/BAR] [--output /FOO/BAR] 
                           [--new-version] [--plot-quicklooks] [-na]
                           SITE [SITE ...]
```

Positional arguments:

| Name   | Description | 
| :---   | :---        |
| `site` |  Site name.|

Optional arguments:

| Short | Long            | Default           | Description                                | 
| :---  | :---            | :---              | :---                                       |
| `-h`  | `--help`        |                   | Show help and exit. |
|       | `--config-dir`  | `./config`        | Path to directory containing config files. |
|       | `--start`       | `current day - 7` | Starting date. |
|       | `--stop`        | `current day - 1 `| Stopping date. |
|       | `--new-version` | `False`             | Create new version of the files. |
| `-na`  | `--no-api`     | `False`             | Disable API calls. Useful for testing. |


### `freeze.py`
Freeze selected files.

```
usage: freeze.py [-h] [--config-dir /FOO/BAR] [-na]
```

Optional arguments:

| Short | Long             | Default      | Description                                | 
| :---  | :---             | :---         | :---                                       |
| `-h`  | `--help`         |              | Show help and exit.                        |
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

