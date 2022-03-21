# ACTRIS Cloudnet data-processing
![](https://github.com/actris-cloudnet/data-processing/workflows/Test%20and%20lint/badge.svg)

Various scripts used in Cloudnet data transfer and processing.

## Installation
The data processing tools are distributed as a docker container as a part of the Cloudnet development toolkit.
Refer to [README of the dev-toolkit repository](https://github.com/actris-cloudnet/dev-toolkit/) on how to set up the CLU development environment.

## Scripts
Once the CLU development environment is running, scripts can be run inside the data-processing container with
the `./run` wrapper.
The scripts are located in `scripts/` folder and should be run from the root: 
```
$ ./run scripts/<script_name.py> --arg1 foo --arg2 bar ...
```
The following scripts are provided:


### `cloudnet.py`
The main wrapper for running all the processing steps.

```
usage: cloudnet.py [-h] -s SITE [-d YYYY-MM-DD] [--start YYYY-MM-DD]
                           [--stop YYYY-MM-DD] [-p ...] COMMAND ...
```

Positional arguments:

| Name      | Description                                                                                                                      | 
|:----------|:---------------------------------------------------------------------------------------------------------------------------------|
| `command` | Command to execute. Must be one of `freeze`, `process`, `model`, `me`, `plot`, or `qc`. Commands are detailed [here](#commands). |

General arguments. These arguments are available for all commands. The arguments must be issued before the command argument.

| Short | Long         | Default           | Description                                                                        | 
|:------|:-------------|:------------------|:-----------------------------------------------------------------------------------|
| `-h`  | `--help`     |                   | Show help and exit.                                                                |
| `-s`  | `--site`     |                   | Site to process data from, e.g, `hyytiala`. Required.                              |
| `-d`  | `--date`     |                   | Single date to be processed. Alternatively, `--start` and `--stop` can be defined. |
|       | `--start`    | `current day - 5` | Starting date.                                                                     |
|       | `--stop`     | `current day `    | Stopping date.                                                                     |
| `-p`  | `--products` | all               | Processed products, e.g, `radar,lidar,categorize,classification`.                  |



### Commands


### `process`

The `process` command processes standard Cloudnet products, such as `radar`, `lidar`, `categorize`, and `classification` products.

In addition to the general arguments, it accepts the following special arguments.

| Short | Long                   | Default | Description                                                                    | 
|:------|:-----------------------|:--------|:-------------------------------------------------------------------------------|
| `-r`  | `--reprocess`          | `False` | See below.                                                                     |
|       | `--reprocess_volatile` | `False` | Reprocess volatile files only (and create new volatile file from unprocessed). |

Behavior of the `--reprocess` flag:

| Existing file                   | `--reprocess`   | Action                                                               |
|:--------------------------------|:----------------|:---------------------------------------------------------------------|
| -                               | `False`         | Create volatile file.                                                |
| -                               | `True`          | Create volatile file.                                                |
| `volatile`                      | `False`         | Reprocess the volatile file (Level 1 products only if new raw data). |
| `volatile`                      | `True`          | Reprocess the volatile file.                                         |
| `stable` (legacy or not)        | `False`         | -                                                                    |
| `stable`                        | `True`          | Create new stable file version.                                      |

### `model`
Create Cloudnet model products.

This command takes no additional arguments

### `me`
Create Cloudnet level 3 model evaluation products (experimental).

Additional arguments:

| Short | Long          | Default | Description                                                           |
|:------|:--------------|:--------|:----------------------------------------------------------------------|
| `-r`  | `--reprocess` | `False` | Process new version of the stable files and reprocess volatile files. |

### `plot`
Don't process anything, only plot images for products.

Additional arguments:

| Short | Long        | Default | Description                                                              |
|:------|:------------|:--------|:-------------------------------------------------------------------------|
| `-m`  | `--missing` | `False` | Only plot images for files that do not have any previous images plotted. |


### `qc`
Don't process anything, only create quality control reports for products.

This command takes no additional arguments

### `freeze`
Freeze selected files by adding a PID to the files and setting their state to `stable`, preventing further changes to the data.

Note: With this script, all sites can be selected using `--site *` argument.

Additional arguments:

| Short | Long      | Default | Description                                                                                                             | 
|:------|:----------|:--------|:------------------------------------------------------------------------------------------------------------------------|
| `-f`  | `--force` | False   | Ignore environment variables `FREEZE_AFTER_DAYS` and `FREEZE_MODEL_AFTER_DAYS`. Allows freezing recently changed files. |


### Examples

Process classification product for the Bucharest site for the date 2021-12-07:

    scripts/cloudnet.py -s bucharest -d 2021-12-07 -p classification process

Plot missing images for Hyytiälä (since 2000-01-01):

    scripts/cloudnet.py -s hyytiala --start 2000-01-01 plot -m

Freeze all files whose measurement date is 2021-01-01 or later:

    scripts/cloudnet.py -s * --start 2021-01-01 freeze

Reprocess all level 2 files between 2021-01-01 and 2021-01-31 for Norunda:

    scripts/cloudnet.py -s norunda --start 2021-01-01 --stop 2021-01-31 -p classification,drizzle,iwc,lwc process -r

## Other scripts

Code that is not involved in the cloudnet data processing chain can be found in other scripts under the `scripts` directory.

### `put-legacy-files.py`

Upload Matlab processed legacy products (`categorize`, and level 2 products) to data portal.

```
usage: put-legacy-files.py [-h] [-y YYYY] PATH
```

Positional arguments:

| Name   | Description                                                            | 
|:-------|:-----------------------------------------------------------------------|
| `path` | Root path of the site containing legacy data, e.g, `/foo/bar/munich/`. |

Optional arguments:

| Short | Long     | Default | Description                     | 
|:------|:---------|:--------|:--------------------------------|
| `-h`  | `--help` |         | Show help and exit.             |
| `-y`  | `--year` | all     | Process only some certain year. |

Behavior:

| Existing file         | Action                                    |
|:----------------------|:------------------------------------------|
| -                     | Add stable legacy file.                   |
| `volatile`            | -                                         |
| `stable` (legacy)     | -                                         |
| `stable` (non-legacy) | Add stable legacy file as oldest version. |



### `map-variable-names.py`
Print list of Cloudnet variables.

```
usage: map-variable-names.py
```

### Development

For development, you may open a bash session inside the container with:

```
$ docker-compose -f ../dev-toolkit/docker-compose.yml run --entrypoint bash data-processing
```

The changes made to the source files on the host computer will be reflected in the container.

### Tests

First, build a separate test container:
```
$ docker build -t test .
```

Run unit tests:
```
$ docker run -tv $PWD/tests:/app/tests -v $PWD/src:/app/src --env-file test.env test pytest
```

Run end-to-end tests:
```
$ docker run -tv $PWD/tests:/app/tests -v $PWD/src:/app/src --env-file e2e-test.env test /bin/sh -c 'for f in tests/e2e/*/main.py; do $f; done'
```


### Licence
MIT

