drop table if exists test.web_pv_uv_temp;
create table test.web_pv_uv_temp
as
select
room_id,domain,begin_time,end_time,
max_viewer,accumulative_viewer_total,
from_qq,live_permission,live_source_type,live_stream_type,
game_id,game_name,play_title, is_live,
sum(case when s_timestamp between begin_time and end_time then 1 else 0 end ) as pv,
count(distinct (case when s_timestamp between begin_time and end_time then uuid else null end)) as uv
from
(select
    room_id,domain,begin_time,end_time,
    max_viewer,accumulative_viewer_total,
    from_qq,live_permission,live_source_type,live_stream_type,
    game_id,game_name,play_title, is_live
from ods.ods_report_play_time_etl
where day='${hiveconf:tby_date}')a
left outer join
(select
    from_unixtime(cast(s_timestamp as bigint)) as s_timestamp ,sbt, uuid
from ods.ods_web_log_page_view
where day between '${hiveconf:tby_date}' and '${hiveconf:y_date}'
and send_count=1)b
on a.room_id=b.sbt
group by room_id,domain,begin_time,end_time,
max_viewer,accumulative_viewer_total,
from_qq,live_permission,live_source_type,live_stream_type,
game_id,game_name,play_title, is_live;



drop table if exists test.pcu_acu_temp;
create table test.pcu_acu_temp
as
select
a.room_id,begin_time,end_time,
max(outer_ol) as pcu_out,
max(inner_ol) as pcu_in,
max(encripted_ol) as pcu_encripted,
nvl(cast(sum(outer_ol)/sum(if(outer_ol>0,1,0)) as decimal),0) as acu_out,
nvl(cast(sum(inner_ol)/sum(if(inner_ol>0,1,0)) as decimal),0) as acu_in,
nvl(cast(sum(encripted_ol)/sum(if(encripted_ol>0,1,0)) as decimal),0) as acu_encripted
from
(select
    room_id,begin_time,end_time
from ods.ods_report_play_time_etl
where day='${hiveconf:tby_date}')a
join
(select time,room_id,outer_ol,inner_ol,encripted_ol
from dw.dw_room_online_count
where day between '${hiveconf:tby_date}' and '${hiveconf:y_date}')b
on a.room_id=b.room_id
where time between begin_time and end_time
group by a.room_id,begin_time,end_time;




drop table if exists test.peak_time_temp;
create table test.peak_time_temp
as
select
t1.room_id,t2.begin_time,t2.end_time,pcu,
min( case when time between begin_time and end_time then time end) as time
from
(select room_id,time, inner_ol
from dw.dw_room_online_count
where day between '${hiveconf:tby_date}' and '${hiveconf:y_date}'
)t1
join
(select a.room_id,begin_time,end_time,
max(case when time between begin_time and end_time then inner_ol else 0 end) as pcu
from
(select
room_id,begin_time,end_time
from ods.ods_report_play_time_etl
where day='${hiveconf:tby_date}')a
left outer join
(select room_id,time,inner_ol
from dw.dw_room_online_count
where day between '${hiveconf:tby_date}' and '${hiveconf:y_date}'
)b
on (a.room_id=b.room_id)
group by a.room_id,begin_time,end_time)t2
on (t1.room_id=t2.room_id and t1.inner_ol=t2.pcu)
group by t1.room_id,t2.begin_time,t2.end_time,pcu;



drop table if exists test.price_item_temp;
create table  test.price_item_temp
as
select a.room_id,begin_time,end_time,
round(sum(price),2) as total_price,
cast(sum(item_num) as int) as total_item_num,
count(distinct operator_id) as total_paying_user_num
from
(select room_id,begin_time,end_time
from ods.ods_report_play_time_etl
where day='${hiveconf:tby_date}')a
join
(select room_id,tr_date, operator_id, price, item_id,item_num
from  ods.ods_cdb_trade
where day between '${hiveconf:tby_date}' and '${hiveconf:y_date}'
and trade_status =1
and room_id>0
and product_name='platform'
and item_id >0)b on a.room_id=b.room_id
where tr_date between begin_time and end_time
group by a.room_id,begin_time,end_time;




drop table if exists test.play_duration_vv_temp;
create table test.play_duration_vv_temp
as
select
    a.room_id, begin_time,end_time,
    nvl(cast(sum(b.play_duration)/
    count(distinct (case when b.play_duration>0
    then deviceid else null end))/1000/60 as decimal),0) as awt,
    sum(case when cast(play_duration as bigint)>0 then 1 else 0 end)as vv,
    sum(case when cast(play_duration as bigint)>=15000 then 1 else 0 end)as valid_vv,
    cast(sum(if(b.play_duration > 0, b.play_duration, 0))/1000/60 as decimal) as play_duration,
    count(distinct (case when b.play_duration > 0 then deviceid else null end)) as vv_device,
    count(distinct (case when b.play_duration >= 15000 then deviceid else null end)) as valid_vv_device
from
    (select room_id,begin_time,end_time
    from ods.ods_report_play_time_etl
    where day='${hiveconf:tby_date}')a
join
    (
    select
        date, roomid as room_id,
        from_unixtime(floor(cast(createtime as bigint)/1000)) as createtime,
        (case
            when substr(ostype, 1, 2)='PC' then playduration
            else
            (playduration - if(stuckduration='NaN',0,stuckduration))
         end)as play_duration,
        deviceid
    from log.playerlog_etl
    where cast(playduration as bigint) between 0 and 86400000
    and cast(stuckduration as bigint)>=0
    and from_unixtime(unix_timestamp(date,'yyyyMMdd'),'yyyy-MM-dd')
    between '${hiveconf:tby_date}' and '${hiveconf:y_date}'
    )b on a.room_id=b.room_id
where createtime between begin_time and end_time
group by a.room_id, begin_time,end_time;





insert overwrite table dw.dw_room_play_time
partition(year='${hiveconf:year}',month='${hiveconf:month}', day='${hiveconf:tby_date}')
select
'${hiveconf:tby_date}'as date,
t1.room_id as room_id, --能显示8位的房间ID
k0.user_id as user_id,
k0.user_title as user_group,
k0.id as category_id,
k0.name as category_name,
k0.cdomain as category_domain,
t1.domain as domain,
t1.begin_time as begin_time,
t1.end_time as end_time,
round((unix_timestamp(t1.end_time) - unix_timestamp(t1.begin_time))/60,2) as play_minutes,
t1.max_viewer as max_viewer,
t1.accumulative_viewer_total as accumulative_viewer_total,
t1.from_qq as from_qq,
t1.live_permission as live_permission,
t1.live_source_type as live_source_type,
t1.live_stream_type as live_stream_type,
t1.game_id as game_id,
t1.game_name as game_name,
t1.play_title as play_title,
t1.is_live as is_live,
nvl(t1.pv,0)as web_pv,
nvl(t1.uv,0)as web_uv,
nvl(t6.app_pv,0)as app_pv,
nvl(t6.app_uv,0)as app_uv,
nvl(t2.pcu_out,0)as pcu_out,
nvl(t2.pcu_in,0)as pcu_in,
nvl(t2.pcu_encripted,0) as pcu_encrypted,
nvl(t2.acu_out,0) as acu_out,
nvl(t2.acu_in,0) as acu_in,
nvl(t2.acu_encripted,0)as acu_encrypted,
t3.time as peak_time,
nvl(t4.total_price,0)as total_price,
nvl(t4.total_item_num,0)as total_item_num,
nvl(t4.total_paying_user_num,0) as total_paying_user_num,
nvl(t5.awt,0) as awt,
nvl(t5.vv,0) as vv,
nvl(t5.valid_vv,0) as valid_vv,
nvl(t5.play_duration,0)as play_duration,
nvl(t5.vv_device,0)as vv_device,
nvl(t5.valid_vv_device,0) as valid_vv_device,
0 as bullet_screen_num


from test.web_pv_uv_temp t1

left outer join
test.pcu_acu_temp t2 on (t1.room_id=t2.room_id and t1.begin_time=t2.begin_time and t1.end_time=t2.end_time)

left outer join
test.peak_time_temp t3 on(t1.room_id=t3.room_id and t1.begin_time=t3.begin_time and t1.end_time=t3.end_time)

left outer join
test.price_item_temp t4 on(t1.room_id=t4.room_id and t1.begin_time=t4.begin_time and t1.end_time=t4.end_time)



left outer join
test.play_duration_vv_temp t5 on(t1.room_id=t5.room_id and t1.begin_time=t5.begin_time and t1.end_time=t5.end_time)

left outer join
(select room_id, app_pv, app_uv
from dw.dw_daily_room_flow
where day='${hiveconf:tby_date}')t6 on (t1.room_id=t6.room_id)

left outer join
(select /*+ MAPJOIN(c) */
r.room_id,r.domain, r.user_id, r.user_title,
c.id, c.name, c.domain as cdomain
from dim.dim_room r left outer join dim.dim_category c on(r.category_id=c.id)
where r.room_id>0)k0
on(t1.room_id=k0.room_id);

