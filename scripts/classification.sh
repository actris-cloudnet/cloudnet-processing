#!/bin/bash

site=$1
date=$2

if [ "$3" == "dl" ]; then
    ./scripts/cloudnet.py -s=$site -d=$date -p radar,lidar,mwr,model,disdrometer fetch
fi

./scripts/cloudnet.py -s=$site -d=$date -p categorize,classification process -r
