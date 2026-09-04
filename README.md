# ACTRIS Cloudnet data processing

[![Test and lint](https://github.com/actris-cloudnet/cloudnet-processing/actions/workflows/test.yml/badge.svg)](https://github.com/actris-cloudnet/cloudnet-processing/actions/workflows/test.yml)

Cloudnet data processing glue code.

## Installation

The data processing tools are distributed as a Docker container as a part of the Cloudnet development toolkit.
Refer to [README of the dev-toolkit repository](https://github.com/actris-cloudnet/dev-toolkit/) on how to set up the CLU development environment.

## Scripts

Once the CLU development environment is running, start the `cloudnet-processing` container by running:

    ./run-bash

The scripts are located in `scripts/` folder and should be run from the root:

    ./scripts/<script_name.py> --arg1 foo --arg2 bar ...

The following scripts are provided:

### `cloudnet.py`

The main wrapper for running all the processing steps.

    usage: cloudnet.py [-h] -s SITES [-p PRODUCTS] [-i INSTRUMENTS] [-m MODELS] [-u UUIDS]
                       [--start YYYY-MM-DD] [--stop YYYY-MM-DD] [-d YYYY-MM-DD]
                       [-c {process,plot,qc,freeze,dvas,fetch,hkd}] [--raw] [--all]
                       [--include-pattern INCLUDE_PATTERN]

Arguments:

| Short | Long                | Default     | Description                                                                                 |
| :---- | :------------------ | :---------- | :------------------------------------------------------------------------------------------ |
| `-h`  | `--help`            |             | Show help and exit.                                                                         |
| `-s`  | `--sites`           |             | Required. E.g, `hyytiala,granada`.                                                          |
| `-p`  | `--products`        |             | E.g. `lidar,classification,l3-cf`.                                                          |
| `-i`  | `--instruments`     |             | E.g. `mira-35,hatpro`.                                                                      |
| `-m`  | `--models`          |             | E.g. `ecmwf,gdas1`.                                                                         |
| `-u`  | `--uuids`           |             | Instrument UUIDs, e.g. `db58480f-58ca-49ad-995c-6c3b89e9a0fc`.                              |
|       | `--start`           | current day | Starting date (included).                                                                   |
|       | `--stop`            | current day | Stopping date (included).                                                                   |
| `-d`  | `--date`            | current day | Single date to be processed. Alternatively, `--start` and `--stop` can be defined.          |
| `-c`  | `--cmd`             | `process`   | Command to be executed.                                                                     |
|       | `--raw`             |             | Fetch raw data excluding `.LV0` files. Only applicable if the command is `fetch`.           |
|       | `--all`             |             | Fetch all raw data including `.LV0` files. Only applicable if the command is `fetch --raw`. |
|       | `--include-pattern` |             | Regex pattern to filter raw files. Only applicable if the command is `fetch`.               |

Shortcut for the `--products` argument:

| Shortcut      | Meaning                                                                                     |
| :------------ | :------------------------------------------------------------------------------------------ |
| `instrument`  | All products of type `instrument` in the data portal, e.g. `radar,lidar,mwr,...`            |
| `geophysical` | All products of type `geophysical` in the data portal, e.g. `categorize,classification,...` |
| `evaluation`  | All products of type `evaluation` in the data portal, e.g. `l3-cf,cpr-simulation,...`       |
| `doppy`       | `doppler-lidar,doppler-lidar-wind,epsilon-lidar`                                            |
| `voodoo`      | `categorize-voodoo,classification-voodoo`                                                   |
| `mwrpy`       | `mwr-l1c,mwr-single,mwr-multi`                                                              |
| `cpr`         | `cpr-simulation,cpr-validation,cpr-tc-validation`                                           |

Notes:

- `--products` has no effect when fetching raw data.

### `submit-data-to-dev.py`

Submit raw instrument or model files to data portal in your development environment.

    usage: submit-data-to-dev.py [-h] -s SITE [-i INSTRUMENT] [--pid PID] [-m MODEL] -d DATE filename [filename ...]

Positional arguments:

| Name       | Description                      |
| :--------- | :------------------------------- |
| `filename` | One or more raw files to submit. |

Options:

| Short | Long           | Description                                                        |
| :---- | :------------- | :----------------------------------------------------------------- |
| `-h`  | `--help`       | Show help and exit.                                                |
| `-s`  | `--site`       | Submit data to site, e.g, `hyytiala`.                              |
| `-d`  | `--date`       | Date to submit, e.g. `2023-10-27`.                                 |
| `-i`  | `--instrument` | Instrument to submit, e.g. `chm15k`. Requires `--pid`.             |
|       | `--pid`        | Instrument PID to submit. Requires `--instrument`.                 |
| `-m`  | `--model`      | Model to submit, e.g. `ecmwf`. Cannot be used with `--instrument`. |

Either `--model` or both `--instrument` and `--pid` must be given.

### `dvas-json.py`

Output file metadata as it would be sent to DVAS.

    usage: dvas-json.py [-h] [--new] file_uuid

Positional arguments:

| Name        | Description                              |
| :---------- | :--------------------------------------- |
| `file_uuid` | Output DVAS metadata for this file UUID. |

Options:

| Short | Long     | Description                                                 |
| :---- | :------- | :---------------------------------------------------------- |
| `-h`  | `--help` | Show help and exit.                                         |
|       | `--new`  | Output metadata in DVAS v3 format instead of the v2 format. |

### `worker.py`

Launch a worker with `./scripts/worker.py` to process incoming _tasks_. Used mainly in production to process data in real-time.

Options:

| Short | Long       | Description                                                                           |
| :---- | :--------- | :------------------------------------------------------------------------------------ |
|       | `--queue ` | Process tasks from this queue, or from default queue if the specified queue is empty. |

### `monitor`

Data quality monitoring tool. See [monitoring README](src/monitoring/README.md) for usage.

### `cronjobs/`

Scripts that are run periodically in production:

| Script                    | Description                                                                |
| :------------------------ | :------------------------------------------------------------------------- |
| `freeze.py`               | Queue freeze tasks for volatile files older than `FREEZE_AFTER_DAYS`.      |
| `yesterdays-qc.py`        | Queue quality control tasks for all files measured yesterday.              |
| `earthcare-validation.py` | Queue CPR validation tasks for sites with an EarthCARE overpass yesterday. |

## Licence

MIT
