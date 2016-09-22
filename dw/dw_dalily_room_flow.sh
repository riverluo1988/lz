#!/bin/bash
if [ $# != 0 ]; then
    y_date=$1
else
#昨天
    y_date=`date +"%Y-%m-%d" -d "-1 days"`
fi

# #前天
# tby_date=`date +"%Y-%m-%d" -d "$y_date -1 days"`

year=`date -d $y_date +"%Y"`
month=`date -d $y_date +"%Y-%m"`

hive_script="/data1/workspace/dw/dw_dalily_room_flow.hql"

echo "hive -hiveconf y_date="${y_date}" -hiveconf year="${year}" -hiveconf  month="${month}" -v -f $hive_script"
hive -hiveconf y_date="${y_date}" -hiveconf year="${year}" -hiveconf  month="${month}" -v -f $hive_script

#cat dw_uid_total.hql
#打印日志？