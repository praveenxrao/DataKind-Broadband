#!/bin/bash

# prompt the user for inputs of type, year and loop through quarters

read -p "Enter year [2019, 2020, 2021]: " YEAR 
#read -p "Enter fixed or mobile: " TYPE

for TYPE in fixed mobile
do
for Q in 1 2 3 4
do
    if [ "$Q" -eq "1" ]
    then
        M=01
    elif [ "$Q" -eq "2" ]
    then
        M=04
    elif [ "$Q" -eq "3" ]
    then
        M=07
    elif [ "$Q" -eq "4" ]
    then
        M=10
    fi
    aws s3 cp s3://ookla-open-data/parquet/performance/type=$TYPE/year=$YEAR/quarter=$Q/$YEAR-$M-01_performance_${TYPE}_tiles.parquet parquet/. --no-sign-request
    echo "copied data from: $TYPE, $YEAR, $Q"
done
done
echo "Done"
exit 0 
