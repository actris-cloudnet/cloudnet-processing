# Actris-cloudnet data-processing
![](https://github.com/actris-cloudnet/data-processing/workflows/Cloudnet%20processing%20CI/badge.svg)

Scripts to run Cloudnet data processing

# Installation
```
$ git clone git@github.com:actris-cloudnet/data-processing.git
$ cd data-processing
$ python3 -m venv venv
$ source venv/bin/activate
(venv) $ pip3 install --upgrade pip
(venv) $ pip3 install .
```

## Syncronizing legacy files
Legacy Cloudnet files lack necessary metadata. Operational-processing offers a script that: 
- Copies source files into other location 
- Fixes their metadata
- Sends metadata to Cloudnet data portal HTTP API

### Usage
Launch from the root folder:
```
$ scripts/sync-folders.py <site_name>
```
Optional arguments:
*  ```--folder FOO ``` Model folder type. Default is ```ecmwf```.
*  ```--input <dir_name> ``` Input directory. Default is ```/ibrix/arch/dmz/cloudnet/data/```.
*  ```--output <dir_name> ``` Output folder. Default is ```/data/clouddata/sites/```.
*  ```-d, --dry ```           Dry run the script for testing the behaviour without writing any files.
*  ```--config-dir <dir_name> ``` Path to directory containing config files. Default: ./config.
*  ```-na, --no-api ```       Disable API calls. Useful for testing.

### Notes
- File type is currently hard coded (```.nc```)
- Files already synchronized are not overwritten.

## Concatenating CHM15k ceilometer files
Some of the CHM15k lidar files come in several files per day while ```cloundetPy``` 
processing requires daily files. A script can be used to generate these daily files. 

### Usage
Launch from the root folder:
```
$ scripts/concat-lidar.py <input_folder>
```
where ```<input_folder> ``` is the root level folder storing the original ceilometer files in 
```input_folder/year/month/day/*.nc``` structure.

Optional arguments:
*  ```-d, --dir <dir_name> ``` Separate folder for the daily files.
* ``` -o, --overwrite``` Overwrite any existing daily files.
* ``` --year YYYY``` Limit to certain year only.
* ``` --month MM``` Limit to certain month only.
* ``` --day DD``` Limit to certain day only.
* ``` -l --limit N``` Run only on folders modified within ```N``` hours . Forces overwriting of daily files.

### Examples
```
$ scripts/concat-lidar.py /data/bucharest/uncalibrated/chm15k/ 
```
This will concatenate, for example, ```/data/bucharest/uncalibrated/chm15k/2020/01/15/*.nc``` into 
```/data/bucharest/uncalibrated/chm15k/2020/chm15k_20200115.nc```, and so on.

After the initial concatenation for all existing folders has been performed, 
it is usually sufficient to use the ```-l``` switch:

```
$ scripts/concat-lidar.py /data/bucharest/uncalibrated/chm15k/ -l=24
```
Which finds the folders updated within 24 hours and overrides daily files from these folders 
making sure they are always up to date (if the script is run daily).

## Process Cloudnet data

### Prerequisites

* Fix the ```input``` and ```output``` data paths in ```config/main.ini``` 
* Make sure that the instrument list is correct in ```config/<site>.ini```.
* Make sure you have ```output/<site>/calibrated/<model>/<year>``` folder containing pre-processed model data. 
* If you use CHM15k lidar, make sure there are pre-processed daily files in ```input/uncalibrated/chm15k/<year>``` folder.

### Usage

Launch the processing script from the root folder:
```
$ scripts/process-cloudnet.py site
```
where ```site``` is one of the ```id```s from ```https://cloudnet.fmi.fi/api/sites/```.

Optional arguments:
* ``` --start YYYY-MM-DD```First day to process (included). Default value is ```current day - 7```.
* ``` --stop YYYY-MM-DD``` Last day to process (not included). Default value is ```current day - 2```.
* ``` --input FOO/BAR```Input folder path. Overwrites ```config/main.ini``` value.
* ``` --output FOO/BAR```Output folder path. Overwrites ```config/main.ini``` value.
* ``` -o, --overwrite``` Overwrites data in existing files.
* ``` -k, --keep_uuid``` Keeps UUID of old file even when ```--overwrite``` is used.

## Tests
Run unit tests
```
$ pytest tests/unit/
```

Run end-to-end tests:
```
$ for f in tests/e2e/*/main.py; do $f; done
```

