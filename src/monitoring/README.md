# Monitoring

## Usage

```bash
# Monitor current period by default
./scripts/monitor {day,week,month,year,all}

# Common options
# start and stop arguments have format YYYY-MM-DD, YYYY-VV, YYYY-MM or YYYY
# depending on the subcommand
--start START               Monitor periods starting from START
--stop STOP                 Monitor periods until STOP
--product [PRODUCT ...]     Monitor only these products
--site [SITE ...]           Monitor only these sites

# Subcommand specific options
## day
--day [YYYY-MM-DD ...]

## week
--week [YYYY-VV ...]

## month
--month [YYYY-MM ...]

## year
--year [YYYY ...]
```
