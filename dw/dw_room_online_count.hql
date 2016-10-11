insert overwrite table dw.dw_room_online_count
partition(year='${hiveconf:year}',month='${hiveconf:month}', day='${hiveconf:y_date}')
select
time,
room_id,
sum(case when period=360 then count else 0 end) as outer_ol,
sum(case when period in(36,38) then count else 0 end) as inner_ol,
sum(case when period=38 then count else 0 end) as encripted_ol
from ods.ods_report_room_online_count_etl
where day = '${hiveconf:y_date}'
and period in(36,38,360)
group by time, room_id;

