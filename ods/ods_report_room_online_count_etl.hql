insert overwrite table ods.ods_report_room_online_count_etl
partition(year='${hiveconf:year}',month='${hiveconf:month}', day='${hiveconf:y_date}')
select
from_unixtime(unix_timestamp(time)) as time,
room_id, category_id, user_title, period,
max(count) as count
from ods.ods_report_room_online_count
where day='${hiveconf:y_date}'
group by time, day,room_id, category_id, user_title, period