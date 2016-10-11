#!/bin/bash

start_date=$1
end_date=$2

while [ ${start_date} \< ${end_date} ]
do
    y_date=${start_date}
    year=${y_date:0:4}
    month=${y_date:0:7}
    echo  "${y_date}"

    hive_script="/data1/workspace/ods/ods_report_room_online_count_etl.hql"

    echo "hive -hiveconf y_date="${y_date}" -hiveconf year="${year}" -hiveconf  month="${month}" -v -f $hive_script"
    hive -hiveconf y_date="${y_date}" -hiveconf year="${year}" -hiveconf  month="${month}" -v -f $hive_script
    start_date=`date -d "${start_date} +1 days" "+%F"`

done