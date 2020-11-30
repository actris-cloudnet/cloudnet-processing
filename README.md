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

```
usage: process-cloudnet.py [-h] [-r] [--config-dir /FOO/BAR] [--start YYYY-MM-DD]
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
|       | `--config-dir`   | `./config`        | Path to directory containing config files. |
|       | `--start`        | `current day - 7` | Starting date. |
|       | `--stop`         | `current day - 1 `| Stopping date. |
| `-p`  | `--products`     | all             | Processed products, e.g, `radar,lidar,categorize,classification`. |

Behavior of `--reprocess` flag:

| Existing file | `--reprocess` | Action          |
| :---          | :---          | :---            |
| -             | `False`       | Create volatile file |
| -             | `True`        | Create stable file |
| `volatile`    | `False`       | Reprocess the volatile file (if new input data) |
| `volatile`    | `True`        | Reprocess the volatile file |
| `stable`      | `False`       | - |
| `stable`      | `True`        | Create new stable file version|



### `freeze.py`
Freeze selected files.

```
usage: freeze.py [-h] [--config-dir /FOO/BAR]
```

Optional arguments:

| Short | Long             | Default     | Description                                | 
| :---  | :---             | :---        | :---                                       |
| `-h`  | `--help`         |             | Show help and exit.                        |
|       | `--config-dir`   | `./config`  | Path to directory containing config files. |


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

