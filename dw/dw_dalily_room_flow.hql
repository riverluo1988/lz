--- 建立临时表来计算pcu,acu
drop table if exists dw.dw_room_online_count_temp;

create table dw.dw_room_online_count_temp
as
select
from_unixtime(unix_timestamp(time)) as time,
day, room_id,
sum(case when period=360 then count else 0 end) as outer_ol,
sum(case when period in(36,38) then count else 0 end) as inner_ol,
sum(case when period=38 then count else 0 end) as encripted_ol
from ods.ods_report_room_online_count_etl
where day = '${hiveconf:y_date}'
and period in(36,38,360)
group by time, day, room_id;



insert overwrite table dw.dw_daily_room_flow
partition(year='${hiveconf:year}',month='${hiveconf:month}', day='${hiveconf:y_date}')
select
'${hiveconf:y_date}' as date,
k1.room_id as room_id, --能显示8位的房间ID
k0.domain as domain,
k0.user_id as user_id,
k0.user_title as user_group,
k0.id as category_id,
k0.name as category_name,
k0.cdomain as category_domain,


k1.web_pv as web_pv,
k1.web_uv as web_uv,
k1.app_pv as app_pv,
k1.app_uv as app_uv,
k1.bullet_screen as bullet_screen_num,
k1.flower as flower_num,


k3.peak_time,
nvl(k2.pcu_out,0) as pcu_out,
nvl(k2.pcu_in,0) as pcu_in,
nvl(k2.pcu_encripted,0) as pcu_encrypted,
nvl(k2.acu_out,1) as acu_out,
nvl(k2.acu_in,1)as acu_in,
nvl(k2.acu_encripted,1)as acu_encrypted,


nvl(k4.total_price,0) as total_price,
nvl(k4.total_item_num,0)as total_item_num,
nvl(k4.total_paying_user_num,0) as total_paying_user_num,
nvl(k5.daily_subscription,0)as daily_subscription_num,
nvl(k8.count,0) as total_subscription_num,

nvl(k6.vv,0) as vv,
nvl(k6.valid_vv,0) as valid_vv,
nvl(k6.awt,0) as awt,

nvl(k7.play_minutes,0) as play_minutes,
if(k7.play_minutes is null,0,1) as is_broadcast,
nvl(k6.play_duration, 0) as play_duration,
nvl(k6.vv_device, 0) as vv_device,
nvl(k6.valid_vv_device, 0) as valid_vv_device

from

(select day,room_id,
sum(case when info_type = 1 then count else 0 end)as web_pv,
sum(case when info_type = 8 then count else 0 end)as web_uv,
sum(case when info_type = 48 then count else 0 end)as app_pv,
sum(case when info_type = 49 then count else 0 end)as app_uv,
sum(case when info_type = 7 then count else 0 end)as bullet_screen,
sum(case when info_type = 5 then count else 0 end)as flower
from ods.ods_report_daily_room_pv_uv
where day='${hiveconf:y_date}'
and info_type in(1,5,7,8,48,49)
group by day,room_id)k1


left outer join


(select day,
room_id,
max(outer_ol) as pcu_out,
max(inner_ol) as pcu_in,
max(encripted_ol)as pcu_encripted,
nvl(cast(sum(outer_ol)/sum(if(outer_ol>0,1,0)) as decimal),0) as acu_out, -- 防止在线为0的分钟拉低平均数
nvl(cast(sum(inner_ol)/sum(if(inner_ol>0,1,0)) as decimal),0) as acu_in,
nvl(cast(sum(encripted_ol)/sum(if(encripted_ol>0,1,0)) as decimal),0) as acu_encripted
from dw.dw_room_online_count_temp
where day = '${hiveconf:y_date}'
group by day,room_id)k2   on(k1.day=k2.day and k1.room_id=k2.room_id)


left outer join


(select
m.day as day,
m.room_id,m.pcu_in,min(m.pcu_time) as peak_time
from
(select
table1.day,
table1.time as pcu_time,
table1.room_id,
table2.pcu_in
from
(select time,
day,room_id, inner_ol
from dw.dw_room_online_count_temp
where day = '${hiveconf:y_date}')table1
join
(select day,room_id,max(inner_ol) as pcu_in
from dw.dw_room_online_count_temp
where day = '${hiveconf:y_date}'
group by day, room_id)table2
on
(table1.day = table2.day and table1.room_id = table2.room_id
and table1.inner_ol=table2.pcu_in))m
group by m.day,m.room_id,m.pcu_in)k3    on(k1.day=k3.day and k1.room_id=k3.room_id)


left outer join


(select day,room_id,
round(sum(price),2) as total_price,
cast(sum(item_num) as int) as total_item_num,
count(distinct operator_id) as total_paying_user_num
from  ods.ods_cdb_trade
where day='${hiveconf:y_date}'
and trade_status =1
and room_id>0
and product_name='platform'
and item_id >0
group by day,room_id)k4   on(k1.day=k4.day and k1.room_id=k4.room_id)


left outer join


(select day, room_id, count(1) as daily_subscription
from ods.ods_room_subscription
where day='${hiveconf:y_date}'
and actived=1
group by day,room_id)k5   on(k1.day=k5.day and k1.room_id=k5.room_id)


left outer join


-- 计算average watch time,playduration是毫秒
-- 如果ostype是PC，有效播放时长就是playduration字段，否则有效播放时长是playduration-stuckduration
-- valid_vv是播放时长大于等于15秒的

(select
    from_unixtime(unix_timestamp(player.date, 'yyyyMMdd'), 'yyyy-MM-dd') as day,
    player.roomid as room_id,
    nvl(cast(sum(player.play_duration)/count(distinct (case when player.play_duration > 0 then deviceid else null end))/1000/60 as decimal), 0) as awt,
    sum(case when cast(play_duration as bigint) > 0 then 1 else 0 end) as vv,
    sum(case when cast(play_duration as bigint) >= 15000 then 1 else 0 end) as valid_vv,
    cast(sum(if(player.play_duration > 0, player.play_duration, 0))/1000/60 as decimal) as play_duration,
    count(distinct (case when player.play_duration > 0 then deviceid else null end)) as vv_device,
    count(distinct (case when player.play_duration >= 15000 then deviceid else null end)) as valid_vv_device
from (
    select
        date,
        roomid,
        deviceid,
        ip,
        case
            when substr(ostype, 1, 2) = 'PC' then playduration
            else (playduration - if(stuckduration = 'NaN', 0, stuckduration))
        end as play_duration
    from log.playerlog_etl
    where cast(playduration as bigint) between 0 and 86400000
    and cast(stuckduration as bigint) >= 0
    and from_unixtime(unix_timestamp(date,'yyyyMMdd'),'yyyy-MM-dd')='${hiveconf:y_date}'
) player
group by player.date, player.roomid) k6 on(k1.day=k6.day and k1.room_id=k6.room_id)


left outer join


-- 计算播放时长，对endtime是0001开头的
-- 结束时间早于开始时间的，处理成开始时间

(select day,room_id,
round(sum(unix_timestamp(
case when
end_time like '0001-01-01%'then concat_ws(' ',to_date(begin_time),'23:59:59')
when
unix_timestamp(end_time)<unix_timestamp(begin_time)
then unix_timestamp(begin_time)
else end_time end)
-unix_timestamp(begin_time))/60) as play_minutes
from ods.ods_report_play_time
where day='${hiveconf:y_date}'
group by day,room_id)k7    on(k1.day=k7.day and k1.room_id=k7.room_id)


left outer join


(select
day, room_id, count
from ods.ods_report_total_room_subscription
where day='${hiveconf:y_date}')k8  on(k1.day=k8.day and k1.room_id=k8.room_id)


left outer join


(select /*+ MAPJOIN(c) */
r.room_id,r.domain, r.user_id, r.user_title,
c.id, c.name, c.domain as cdomain
from dim.dim_room r left outer join dim.dim_category c on(r.category_id=c.id)
where r.room_id>0)k0
on(k1.room_id=k0.room_id);













