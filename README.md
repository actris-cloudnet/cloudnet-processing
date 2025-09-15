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

    usage: cloudnet.py [-h] [-s SITES] [-p PRODUCTS] [-i INSTRUMENTS] [-m MODELS] [-u UUIDS]
                       [--start YYYY-MM-DD] [--stop YYYY-MM-DD] [-d YYYY-MM-DD]
                       [-c {process,plot,qc,freeze,dvas,fetch,hkd}] [--raw] [--all]

Arguments:

| Short | Long            | Default     | Description                                                                        |
| :---- | :-------------- | :---------- | :--------------------------------------------------------------------------------- |
| `-h`  | `--help`        |             | Show help and exit.                                                                |
| `-s`  | `--sites`       |             | E.g, `hyytiala,granada`.                                                           |
| `-p`  | `--products`    |             | E.g. `lidar,classification,l3-cf`.                                                 |
| `-i`  | `--instruments` |             | E.g. `mira-35,hatpro`.                                                             |
| `-m`  | `--models`      |             | E.g. `ecmwf,gdas1`.                                                                |
| `-u`  | `--uuids`       |             | Instrument UUIDs, e.g. `db58480f-58ca-49ad-995c-6c3b89e9a0fc`.                     |
|       | `--start`       |             | Starting date (included).                                                          |
|       | `--stop`        | current day | Stopping date (included).                                                          |
| `-d`  | `--date`        | current day | Single date to be processed. Alternatively, `--start` and `--stop` can be defined. |
| `-c`  | `--cmd`         | `process`   | Command to be executed.                                                            |
|       | `--raw`         |             | Fetch raw data excluding .lv0 files. Only applicable if the command is `fetch`.    |
|       | `--all`         |             | Fetch all raw data. Only applicable if the command is `fetch --raw`.               |

Shortcut for the `--products` argument:

| Shortcut      | Meaning                                                                                                                                         |
| :------------ | :---------------------------------------------------------------------------------------------------------------------------------------------- |
| `instrument`  | `disdrometer,doppler-lidar,doppler-lidar-wind,epsilon-lidar,lidar,mwr,mwr-l1c,mwr-multi,mwr-single,radar,rain-gauge,rain-radar,weather-station` |
| `geophysical` | `categorize,categorize-voodoo,classification,classification-voodoo,iwc,lwc,drizzle,ier,der`                                                     |
| `evaluation`  | `cpr-simulation,l3-cf,l3-iwc,l3-lwc`                                                                                                            |
| `doppy`       | `doppler-lidar,doppler-lidar-wind,epsilon-lidar`                                                                                                |
| `voodoo`      | `categorize-voodoo,classification-voodoo`                                                                                                       |
| `mwrpy`       | `mwr-l1c,mwr-single,mwr-multi`                                                                                                                  |
| `cpr`         | `cpr-simulation`                                                                                                                                |

Notes:

- `--products` has no effect when fetching raw data.

### `submit-data-to-dev.py`

Submit raw files to data portal in your development environment.

    usage: submit-data-to-dev.py [-h] -s SITE -i INSTRUMENT -d DATE --pid PID filename

Positional arguments:

| Name       | Description         |
| :--------- | :------------------ |
| `filename` | Raw file to submit. |

Options:

| Short | Long           | Description                           |
| :---- | :------------- | :------------------------------------ |
| `-h`  | `--help`       | Show help and exit.                   |
| `-s`  | `--site`       | Submit data to site, e.g, `hyytiala`. |
| `-d`  | `--date`       | Date to submit, e.g. `2023-10-27`.    |
| `-i`  | `--instrument` | Instrument to submit, e.g. `chm15k`.  |
|       | `--pid`        | Instrument PID to submit.             |

### `dvas-json.py`

Output file metadata as it would be sent to DVAS.

    usage: dvas-json.py [-h] file_uuid

Positional arguments:

| Name        | Description                              |
| :---------- | :--------------------------------------- |
| `file_uuid` | Output DVAS metadata for this file UUID. |

Options:

| Short | Long     | Description         |
| :---- | :------- | :------------------ |
| `-h`  | `--help` | Show help and exit. |

### `worker.py`

Launch a worker with `./scripts/worker.py` to process incoming _tasks_. Used mainly in production to process data in real-time.

Options:

| Short | Long       | Description                                                                           |
| :---- | :--------- | :------------------------------------------------------------------------------------ |
|       | `--queue ` | Process tasks from this queue, or from default queue if the specified queue is empty. |

## Licence

MIT
