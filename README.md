# download_wunderground
Tool to download wunderground weather data and convert to netCDF

## Installation
download_wunderground is installable via pip:
```
pip install git+https://github.com/ERA-URBAN/download_wunderground
```
download_wunderground depends on the following packages:
```
lxml
numpy
ConfigArgParse
dateutils
netCDF4
```

## Usage
```
usage: download_wunderground [-h] [-o OUTPUTDIR] [--TMP_DIR TMP_DIR]
                             [-b STARTDATE] [-e ENDDATE] [-s STATIONID]
                             [-c CSVFILE] [-k]
                             [-l {debug,info,warning,critical,error}]

Combine csv files weather underground in one output file

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUTDIR, --outputdir OUTPUTDIR
                        Data output directory (defaults to CWD)
  --TMP_DIR TMP_DIR     Directory where intermediate files are saved, defaults
                        to DOWNLOAD_DIR
  -b STARTDATE, --startdate STARTDATE
                        Start date YYYYMMDD
  -e ENDDATE, --enddate ENDDATE
                        End date YYYYMMDD
  -s STATIONID, --stationid STATIONID
                        Station id
  -c CSVFILE, --csvfile CSVFILE
                        CSV data file containing station information
  -k, --keep            Keep downloaded files
  -l {debug,info,warning,critical,error}, --log {debug,info,warning,critical,error}
                        Log level
```
