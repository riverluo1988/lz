#!/bin/bash

start_date=$1
end_date=$2

while [ ${start_date} \< ${end_date} ]
do
    y_date=${start_date}
    tby_date=`date +"%Y-%m-%d" -d "$y_date -1 days"`
    year=${y_date:0:4}
    month=${y_date:0:7}
    echo  "${tby_date}"

    hive_script="/data1/workspace/dw/dw_room_play_time_v2.hql"

    echo "hive -hiveconf y_date="${y_date}" -hiveconf tby_date="${tby_date}" -hiveconf year="${year}" -hiveconf  month="${month}" -v -f $hive_script"

    hive -hiveconf y_date="${y_date}" -hiveconf tby_date="${tby_date}" -hiveconf year="${year}" -hiveconf  month="${month}" -v -f $hive_script

    start_date=`date -d "${start_date} +1 days" "+%F"`

done