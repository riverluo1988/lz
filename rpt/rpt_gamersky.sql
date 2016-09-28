create table if not exists rpt.rpt_gamersky_user_analysis(
date                string,
create_time         string,
uv                  bigint,
new_user_cnt        bigint comment '新用户数',
frequent_user_cnt   bigint comment '老用户数',
frequent_user_ratio double comment'存量用户比',
ip                  bigint,
stay_seconds        bigint comment'用户停留时长',
bounce_rate         double comment'页面跳出率',
1_day_retention_rate double comment'次日留存率',
7_days_retention_rate double comment'7日留存率')
comment '游民星空用户分析'
;

insert into table rpt.rpt_gamersky_user_analysis
select '${hiveconf:y_date}' as date,
unix_timestamp() as create_time,
sum(uv) uv,
sum(new_user_cnt) new_user_cnt,
sum(frequent_user_cnt) frequent_user_cnt,
sum(frequent_user_ratio) frequent_user_ratio,
sum(ip) ip,
sum(stay_seconds) stay_seconds,
sum(bounce_rate) bounce_rate,
sum(1_day_retention_rate) 1_day_retention_rate,
sum(7_days_retention_rate) 7_days_retention_rate
from
(
-- uv new_user
select uv, new_user_cnt, uv - new_user_cnt as frequent_user_cnt,
round((uv-new_user_cnt)/uv,4)as frequent_user_ratio,
0 as ip, 0 as stay_seconds, 0 as bounce_rate,
0 as 1_day_retention_rate, 0 as 7_days_retention_rate
from
(select count(distinct (case when is_new=1 then uuid else null end))as new_user_cnt,
count(distinct uuid) as uv
from dw.dw_user_pv_track
where day = '${hiveconf:y_date}'
and source_url rlike 'gamersky.com'
and url rlike 'longzhu.com'
)a

union all

-- ip
select 0 as uv, 0 as new_user_cnt, 0 as frequent_user_cnt, 0 as frequent_user_ratio,
count(distinct ip) as ip, 0 as stay_seconds, 0 as bounce_rate,
0 as 1_day_retention_rate, 0 as 7_days_retention_rate
from ods.ods_web_log_page_view
where day = '${hiveconf:y_date}'
and source_url rlike 'gamersky.com'
and url rlike 'longzhu.com'

union all
-- duration

select 0 as uv, 0 as new_user_cnt, 0 as frequent_user_cnt, 0 as frequent_user_ratio,
0 as ip,
sum(page_stay) as stay_seconds, 0 as bounce_rate,
0 as 1_day_retention_rate, 0 as 7_days_retention_rate
from dw.dw_user_pv_track
where day = '${hiveconf:y_date}'
and source_url rlike 'gamersky.com'
and url rlike 'longzhu.com'

union all
-- bounce rate

select 0 as uv, 0 as new_user_cnt, 0 as frequent_user_cnt, 0 as frequent_user_ratio,
0 as ip, 0 as stay_seconds,
round(sum(case when max_seq_no_in_session=1 then 1 else 0 end)/count(1),4) as bounce_rate,
0 as 1_day_retention_rate, 0 as 7_days_retention_rate
from
(select distinct uuid, session_id_in_day,max_seq_no_in_session
from
(select  uuid, session_id_in_day,
max(seq_no_in_session) over(partition by uuid, session_id_in_day)as max_seq_no_in_session
from dw.dw_user_pv_track
where day = '${hiveconf:y_date}'
and source_url rlike 'gamersky.com'
and url rlike 'longzhu.com'
)t)m

union all
-- 次日留存率

select 0 as uv, 0 as new_user_cnt, 0 as frequent_user_cnt, 0 as frequent_user_ratio,
0 as ip, 0 as stay_seconds, 0 as bounce_rate,
round(count(b.uuid)/count(a.uuid),4) as 1_day_retention_rate,
0 as 7_days_retention_rate
from
(select distinct uuid
from dw.dw_user_pv_track
where day = date_sub('${hiveconf:y_date}',1)
and source_url rlike 'gamersky.com'
and url rlike 'longzhu.com'
and is_new=1)a

left outer join
(select distinct uuid
from dw.dw_user_pv_track
where day = '${hiveconf:y_date}'
and source_url rlike 'gamersky.com'
and url rlike 'longzhu.com')b on (a.uuid=b.uuid)


union all
-- 七日留存

select 0 as uv, 0 as new_user_cnt, 0 as frequent_user_cnt, 0 as frequent_user_ratio,
0 as ip, 0 as stay_seconds, 0 as bounce_rate,
0 as 1_day_retention_rate,
round(count(b.uuid)/count(a.uuid),4) as 7_days_retention_rate
from
(select distinct uuid
from dw.dw_user_pv_track
where day = date_sub('${hiveconf:y_date}',7)
and source_url rlike 'gamersky.com'
and url rlike 'longzhu.com'
and is_new=1)a

left outer join
(select distinct uuid
from dw.dw_user_pv_track
where day = '${hiveconf:y_date}'
and source_url rlike 'gamersky.com'
and url rlike 'longzhu.com')b on (a.uuid=b.uuid)
)all;

