-- 建立临时表来计算pcu
create table dw.dw_online_temp
as
select oc.time, oc.dt, oc.room_id,
sum(case when period=360 then count else 0 end) as outer_ol,
sum(case when period in(36,38) then count else 0 end) as inner_ol,
sum(case when period=38 then count else 0 end) as encripted_ol
from ods.ods_room_online_count oc
where dt='20160825'
and period in(36,38,360)
group by oc.time, oc.dt, oc.room_id;




select
'2016-08-25' as date,
k0.room_id as room_id,
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

nvl(k6.awt,0) as awt,
nvl(k2.pcu_out,0) as pcu_out,
nvl(k2.pcu_in,0) as pcu_in,
nvl(k2.pcu_encripted,0) as pcu_encrypted,
nvl(k2.acu_out,1) as acu_out,
nvl(k2.acu_in,1)as acu_in,
nvl(k2.acu_encripted,1)as acu_encrypted,

k3.peak_time,

nvl(k4.total_price,0) as total_price,
nvl(k4.total_tools_num,0)as total_tools_num,
nvl(k4.total_paying_user,0) as total_paying_user_num,
nvl(k5.daily_subscription,0)as daily_subscription_num,
0 as total_subscription_num

from
--计算pv，uv,弹幕数
(select pu.day,pu.room_id,
sum(case when info_type = 1 then count else 0 end)as web_pv,
sum(case when info_type = 8 then count else 0 end)as web_uv,
sum(case when info_type = 48 then count else 0 end)as app_pv,
sum(case when info_type = 49 then count else 0 end)as app_uv,
sum(case when info_type = 7 then count else 0 end)as bullet_screen
from ods.ods_report_daily_room_pv_uv pu
where pu.day='2016-08-25'
and info_type in(1,7,8,48,49)
group by pu.day,pu.room_id)k1

left outer join
-- 计算pcu
(select
from_unixtime(unix_timestamp(dw_online_temp.dt,'yyyyMMdd'),'yyyy-MM-dd') as day,
dw_online_temp.room_id,
max(outer_ol) as pcu_out,
max(inner_ol) as pcu_in,
max(encripted_ol)as pcu_encripted,
nvl(cast(sum(outer_ol)/sum(if(outer_ol>0,1,0)) as decimal),0) as acu_out, -- 排除在线为0的时刻
nvl(cast(sum(inner_ol)/sum(if(inner_ol>0,1,0)) as decimal),0) as acu_in,
nvl(cast(sum(encripted_ol)/sum(if(encripted_ol>0,1,0)) as decimal),0) as acu_encripted
from dw.dw_online_temp dw_online_temp
where dt='20160825'
group by dw_online_temp.dt, dw_online_temp.room_id)k2  on(k1.day=k2.day and k1.room_id=k2.room_id)

left outer join
-- 计算pcu的时间点
(select
from_unixtime(unix_timestamp(m.dt,'yyyyMMdd'),'yyyy-MM-dd') as day,
m.room_id,m.pcu_in,min(m.pcu_time) as peak_time
from
(select
table1.dt,
from_unixtime(table1.time,'yyyy-MM-dd HH:mm:ss') as pcu_time,
table1.room_id,
table2.pcu_in
from
(select a.time,
a.dt,a.room_id,a.inner_ol
from dw.dw_online_temp a
where dt='20160825')table1
join
(select a.dt,a.room_id,max(inner_ol) as pcu_in
from dw.dw_online_temp a
where dt='20160825'
group by a.dt,a.room_id)table2
on
(table1.dt=table2.dt and table1.room_id=table2.room_id
and table1.inner_ol=table2.pcu_in))m
group by m.dt,m.room_id,m.pcu_in)k3  on(k1.day=k3.day and k1.room_id=k3.room_id)

left outer join
-- 计算房间花费流水
(select trade.day,trade.room_id,
round(sum(trade.price),2) as total_price,
cast(sum(trade.item_num) as int) as total_tools_num,
count(distinct operatorid) as total_paying_user
from  ods.ods_cdb_trade trade
where day='2016-08-25'
and trade_status =1  --交易成功
 and trade.room_id>0  --有效房间
 and product_name='platform' --直播平台
and item_id >0   --有效道具
group by trade.day,trade.room_id)k4  on(k1.day=k4.day and k1.room_id=k4.room_id)

left outer join
-- 计算订阅数
(select day,roomid as room_id,count(*) as daily_subscription
from ods.ods_room_subscription
where day='2016-08-25'
and actived='true'
group by day,roomid)k5 on(k1.day=k5.day and k1.room_id=k5.room_id)

left outer join
-- 计算average watch time
-- 如果ostype是PC，有效播放时长就是playduration字段，否则有效播放时长是playduration-stuckduration
-- playduration是毫秒，转换成分钟
(select
from_unixtime(unix_timestamp(player.date,'yyyyMMdd'),'yyyy-MM-dd') as day,
player.roomid as room_id,
cast(sum(player.play_duration)/count(distinct player.deviceid)/1000/60 as decimal) as awt
from
(select date,roomid,
(case when ostype='PC' then playduration
else (playduration - if(stuckduration='NaN',0,stuckduration))end) as play_duration,
deviceid
from  log.playerlog_etl
where cast(playduration as bigint)<=86400000 --排除播放时长大于一天的记录
and date='20160826'
and ostype in('PC','A','I'))player --pc,android,iphone
where player.play_duration>0
group by player.date,player.roomid)k6  on(k1.day=k6.day and k1.room_id=k6.room_id)

left outer join

(select /*+ MAPJOIN(c) */
r.room_id,r.domain, r.user_id, r.user_title,
c.id, c.name, c.domain as cdomain
from dim.dim_room r left outer join dim.dim_category c on(r.category_id=c.id))k0
on(k1.room_id=k0.room_id)





