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
usage: process-cloudnet.py [-h] [-r] [--config-dir /FOO/BAR] [-d YYYY-MM-DD] [--start YYYY-MM-DD]
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
usage: process-model.py [-h] [--config-dir /FOO/BAR] SITE
```

Positional arguments:

| Name   | Description | 
| :---   | :---        |
| `site` |  Site name.|

Optional arguments:

| Short | Long             | Default           | Description                                | 
| :---  | :----------             | :---              | :---                                       |
| `-h`  | `--help`         |                   | Show help and exit. |
|       | `--config-dir`   | `./config`        | Path to directory containing config files. |



### `put-legacy-files.py`

Upload Matlab processed legacy products (`categorize`, and level 2 products) to data portal.

```
usage: put-legacy-files.py [-h] [-y YYYY] [--config-dir /FOO/BAR] PATH
```

Positional arguments:

| Name   | Description | 
| :---   | :---        |
| `path` | Root path of the site containing legacy data, e.g, `/foo/bar/munich/`. |

Optional arguments:

| Short | Long             | Default     | Description                                | 
| :---  | :---             | :---        | :---                                       |
| `-h`  | `--help`         |             | Show help and exit.                        |
|       | `--config-dir`   | `./config`  | Path to directory containing config files. |
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

