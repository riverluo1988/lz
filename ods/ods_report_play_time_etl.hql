add file combine2.py;

insert overwrite table ods.ods_report_play_time_etl
partition(year='${hiveconf:year}',month='${hiveconf:month}', day='${hiveconf:y_date}')
select transform(day,room_id,domain,begin_time,end_time,max_viewer,accumulative_viewer_total,execute_times,begin_flower_count,end_flower_count,game_id,game_name,play_title,is_live)
using 'python combine2.py'as (day,room_id,domain,begin_time,end_time,max_viewer,accumulative_viewer_total,execute_times,begin_flower_count,end_flower_count,game_id,game_name,play_title,is_live)
from(
select day,room_id,domain,
from_unixtime(unix_timestamp(begin_time)) as begin_time,
(case when
end_time like '0001-01-01%'then concat_ws(' ',to_date(begin_time),'23:59:59')
when
to_date(end_time)!=to_date(begin_time) then concat_ws(' ',to_date(begin_time),'23:59:59')
when
unix_timestamp(end_time)<unix_timestamp(begin_time) then
from_unixtime(unix_timestamp(begin_time))
else from_unixtime(unix_timestamp(end_time)) end) as end_time,
max_viewer,accumulative_viewer_total,execute_times,begin_flower_count,end_flower_count,
game_id,game_name,play_title,
if(substring(play_title,1,4) like'%重播%','replay','live')as is_live
from ods.ods_report_play_time
where day='${hiveconf:y_date}' order by room_id, begin_time)a;











