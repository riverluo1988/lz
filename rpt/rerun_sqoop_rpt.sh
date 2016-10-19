#!/usr/bin/bash

str_date=$1
end_date=$2
while [ ${str_date} \< ${end_date} ]
do
    year=${str_date:0:4}
    month=${str_date:0:7}
    echo  "${str_date}"
    sqoop export --connect "jdbc:mysql://192.168.7.25/report"           --username report --password clt3BUzhrAc5C7dxEpmL --table rpt_daily_channel_analysis_report  --hcatalog-database rpt --hcatalog-table rpt_daily_channel_analysis   --hcatalog-partition-keys year,month,day --hcatalog-partition-values ${year},${month},${str_date}
    str_date=`date -d "${str_date} +1 days" "+%F"`
done

