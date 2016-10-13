create table test.test_daily_play_time
as
select
    k1.day,
    k1.room_id as room_id, --能显示8位的房间ID
    k0.user_id as user_id,
    k0.user_title as user_group,
    k0.id as category_id,
    k0.name as category_name,
    k0.cdomain as category_domain,
    k1.domain as domain,
    k1.begin_time as begin_time,
    k1.end_time as end_time,
    round((unix_timestamp(k1.end_time) - unix_timestamp(k1.begin_time))/60,0) as play_minutes,
    k1.max_viewer as max_viewer,
    k1.accumulative_viewer_total as accumulative_viewer_total,
    k1.from_qq as from_qq,
    k1.live_permission as live_permission,
    k1.live_source_type as live_source_type,
    k1.live_stream_type as live_stream_type,
    k1.game_id as game_id,
    k1.game_name as game_name,
    k1.play_title as play_title,
    k1.is_live as is_live,
    nvl(k1.pv,0)as web_pv,
    nvl(k1.uv,0)as web_uv,
    nvl(k6.app_pv,0)as app_pv,
    nvl(k6.app_uv,0)as app_uv,
    nvl(k2.pcu_out,0)as pcu_out,
    nvl(k2.pcu_in,0)as pcu_in,
    nvl(k2.pcu_encripted,0) as pcu_encrypted,
    nvl(k2.acu_out,0) as acu_out,
    nvl(k2.acu_in,0) as acu_in,
    nvl(k2.acu_encripted,0)as acu_encrypted,
    k3.time as peak_time,
    nvl(k4.total_price,0)as total_price,
    nvl(k4.total_item_num,0)as total_item_num,
    nvl(k4.total_paying_user_num,0) as total_paying_user_num,
    nvl(k5.awt,0) as awt,
    nvl(k5.vv,0) as vv,
    nvl(k5.valid_vv,0) as valid_vv,
    nvl(k5.play_duration,0)as play_duration,
    nvl(k5.vv_device,0)as vv_device,
    nvl(k5.valid_vv_device,0) as valid_vv_device,
    0 as bullet_screen_num
from
    (select
    day,room_id,domain,begin_time,end_time,
    max_viewer,accumulative_viewer_total,
    from_qq,live_permission,live_source_type,live_stream_type,
    game_id,game_name,play_title, is_live,
    count(1) as pv, count(distinct uuid) as uv
    from
    (select day,room_id,domain,begin_time,end_time,
    max_viewer,accumulative_viewer_total,
    from_qq,live_permission,live_source_type,live_stream_type,
    game_id,game_name,play_title, is_live
    from ods.ods_report_play_time_etl
    where day='2016-09-25' and room_id=19569)a
    left outer join
    (select from_unixtime(cast(s_timestamp as bigint)) as s_timestamp ,sbt, uuid
    from ods.ods_web_log_page_view
    where day='2016-09-25' and sbt=19569
    and send_count=1)b
    on a.room_id=b.sbt
    where s_timestamp between begin_time and end_time
    group by day,room_id,domain,begin_time,end_time,
    max_viewer,accumulative_viewer_total,
    from_qq,live_permission,live_source_type,live_stream_type,
    game_id,game_name,play_title, is_live)k1

left outer join

    (select a.room_id,begin_time,end_time,
    max(outer_ol) as pcu_out,
    max(inner_ol) as pcu_in,
    max(encripted_ol) as pcu_encripted,
    nvl(cast(sum(outer_ol)/sum(if(outer_ol>0,1,0)) as decimal),0) as acu_out,
    nvl(cast(sum(inner_ol)/sum(if(inner_ol>0,1,0)) as decimal),0) as acu_in,
    nvl(cast(sum(encripted_ol)/sum(if(encripted_ol>0,1,0)) as decimal),0) as acu_encripted
    from
    (select room_id,begin_time,end_time
    from ods.ods_report_play_time_etl
    where day='2016-09-25')a
    join
    (select time,room_id,outer_ol,inner_ol,encripted_ol
    from dw.dw_room_online_count
    where day='2016-09-25')b
    on a.room_id=b.room_id
    where time between begin_time and end_time
    group by a.room_id,begin_time,end_time)k2
    on (k1.room_id=k2.room_id and k1.begin_time=k2.begin_time)

left outer join

    (select time,room_id,inner_ol
    from dw.dw_room_online_count
    where day='2016-09-25')k3
    on (k3.room_id=k2.room_id and k3.inner_ol=k2.pcu_in)


left outer join

    (
    select a.room_id,begin_time,end_time,
    round(sum(price),2) as total_price,
    cast(sum(item_num) as int) as total_item_num,
    count(distinct operator_id) as total_paying_user_num
    from
    (select room_id,begin_time,end_time
    from ods.ods_report_play_time_etl
    where day='2016-09-25')a
    join
    (select room_id,tr_date, operator_id, price, item_id,item_num
    from  ods.ods_cdb_trade
    where day between '2016-09-25' and '2016-09-26'
    and trade_status =1
    and room_id>0
    and product_name='platform'
    and item_id >0)b on a.room_id=b.room_id
    where tr_date between begin_time and end_time
    group by a.room_id,begin_time,end_time)k4
    on(k4.room_id=k1.room_id and k4.begin_time=k1.begin_time)

left outer join

    (select
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
        where day='2016-09-25')a
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
        and from_unixtime(unix_timestamp(date,'yyyyMMdd'),'yyyy-MM-dd')='2016-09-25'
        )b on a.room_id=b.room_id
    where createtime between begin_time and end_time
    group by a.room_id, begin_time,end_time)k5
    on (k1.room_id=k5.room_id and k1.begin_time=k5.begin_time)


left outer join

(select room_id, app_pv, app_uv
from dw.dw_daily_room_flow
where day='2016-09-25')k6 on (k1.room_id=k6.room_id)

left outer join

(select /*+ MAPJOIN(c) */
r.room_id,r.domain, r.user_id, r.user_title,
c.id, c.name, c.domain as cdomain
from dim.dim_room r left outer join dim.dim_category c on(r.category_id=c.id)
where r.room_id>0)k0
on(k1.room_id=k0.room_id);


