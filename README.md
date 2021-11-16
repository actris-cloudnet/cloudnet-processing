# ACTRIS Cloudnet data-processing
![](https://github.com/actris-cloudnet/data-processing/workflows/Test%20and%20lint/badge.svg)

Various scripts used in Cloudnet data transfer and processing.

### Installation
The data processing tools are distributed as a docker container as a part of the Cloudnet development toolkit.
Refer to [README of the dev-toolkit repository](https://github.com/actris-cloudnet/dev-toolkit/) on how to set up the CLU development environment.

### Scripts
Once the CLU development environment is running, scripts can be run inside the data-processing container with
the `./run` wrapper.
The scripts are located in `scripts/` folder and should be run from the root: 
```
$ ./run scripts/<script_name.py> arg1 --arg2=foo ...
```
The following scripts are provided:


### `process-cloudnet.py`
Create Cloudnet products.

```
usage: process-cloudnet.py [-h] [-r] [--reprocess_volatile] [-d YYYY-MM-DD] [--start YYYY-MM-DD]
                           [--stop YYYY-MM-DD] [-p ...] SITE
```

Positional arguments:

| Name   | Description | 
| :---   | :---        |
| `site` |  Site name.|

Optional arguments:

| Short | Long             | Default           | Description                                | 
| :---  | :----------             | :---              | :---                                       |
| `-h`  | `--help`         |                   | Show help and exit. |
| `-r`  | `--reprocess`    | `False`           | See below. |
|       | `--reprocess_volatile`  | `False`    | Reprocess volatile files only (and create new volatile file from unprocessed). |
| `-d`  | `--date`         |                   | Single date to be processed. Alternatively `--start` and `--stop` can be defined.|
|       | `--start`        | `current day - 7` | Starting date. |
|       | `--stop`         | `current day - 1 `| Stopping date. |
| `-p`  | `--products`     | all             | Processed products, e.g, `radar,lidar,categorize,classification`. |

Behavior of the `--reprocess` flag:

| Existing file | `--reprocess` | Action          |
| :---          | :---          | :---            |
| -             | `False`       | Create volatile file. |
| -             | `True`        | Create volatile file. |
| `volatile`    | `False`       | Reprocess the volatile file (Level 1 products only if new raw data).|
| `volatile`    | `True`        | Reprocess the volatile file. |
| `stable` (legacy or not)      | `False`       | - |
| `stable`      | `True`        | Create new stable file version.|

### `process-model.py`
Create Cloudnet model products.

```
usage: process-model.py [-h] SITE
```

Positional arguments:

| Name   | Description | 
| :---   | :---        |
| `site` |  Site name.|

Optional arguments:

| Short | Long             | Default           | Description                                | 
| :---  | :----------             | :---              | :---                                       |
| `-h`  | `--help`         |                   | Show help and exit. |



### `put-legacy-files.py`

Upload Matlab processed legacy products (`categorize`, and level 2 products) to data portal.

```
usage: put-legacy-files.py [-h] [-y YYYY] PATH
```

Positional arguments:

| Name   | Description | 
| :---   | :---        |
| `path` | Root path of the site containing legacy data, e.g, `/foo/bar/munich/`. |

Optional arguments:

| Short | Long             | Default     | Description                                | 
| :---  | :---             | :---        | :---                                       |
| `-h`  | `--help`         |             | Show help and exit.                        |
|  `-y` | `--year`         | all         | Process only some certain year.            |

Behavior:

| Existing file          | Action          |
| :---                   | :---            |
| -                      | Add stable legacy file. |
| `volatile`             | - |
| `stable` (legacy)      | - |
| `stable` (non-legacy)  | Add stable legacy file as oldest version. |


### `freeze.py`
Freeze selected files.

```
usage: freeze.py [-h]
```

Optional arguments:

| Short | Long             | Default     | Description                                | 
| :---  | :---             | :---        | :---                                       |
| `-h`  | `--help`         |             | Show help and exit.                        |


### `map-variable-names.py`
Print list of Cloudnet variables.

```
usage: map-variable-names.py
```

### Tests
First, build the docker container:
```
$ docker build -t test .
```

Run unit tests:
```
$ docker run --env-file test.env test pytest
```

Run end-to-end tests:
```
$ docker run --env-file e2e-test.env test /bin/sh -c 'for f in tests/e2e/*/main.py; do $f; done'
```

### Licence
MIT

