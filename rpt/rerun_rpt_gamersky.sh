#!/bin/bash
start_date=$1
end_date=$2

while [ ${start_date} \< ${end_date} ]
do

# if [[ $# != 0]]; then
#     y_date=$1
# else
# #昨天
#     y_date=`date +"%Y-%m-%d" -d "-1 days"`
# fi

# #前天
# tby_date=`date +"%Y-%m-%d" -d "$y_date -1 days"`
# year=`date -d $y_date +"%Y"`
# month=`date -d $y_date +"%Y-%m"`
    y_date=${start_date}
    hive_script="/data1/workspace/rpt/rpt_gamersky.hql"

    echo "hive -hiveconf y_date="${y_date}" -v -f $hive_script"
    hive -hiveconf y_date="${y_date}"  -v -f $hive_script
    start_date=`date -d "${start_date} +1 days" "+%F"`
done


