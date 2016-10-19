insert overwrite table rpt.rpt_daily_channel_analysis
partition(year='${hiveconf:year}',month='${hiveconf:month}', day='${hiveconf:y_date}')

select
'${hiveconf:y_date}' as date,
concat(if(t1.pv_type=1,'内嵌','跳转'),'_',t1.site_type)as channel_name,
ip, uv, pv, stay_hours, new_user_cnt, frequent_user_cnt,
frequent_user_ratio,bounce_rate,
1_day_retention_rate,7_days_retention_rate

from
(-- ip, uv, new_user, stay_hours
select
    pv_type, site_type,
    ip, uv, pv, stay_hours, new_user_cnt, (uv - new_user_cnt) as frequent_user_cnt,
    round((uv-new_user_cnt)/uv,4)as frequent_user_ratio
from
    (select pv_type,site_type,
    count(distinct (case when to_date(first_login_time)=day then uuid else null end)) as new_user_cnt,
    count(distinct ip) as ip,
    count(1) as pv,
    count(distinct uuid) as uv,
    round(sum(page_stay)/3600,2) as stay_hours
    from dw.dw_user_pv_track
    where day = '${hiveconf:y_date}'
    and pv_type in(1,2)
    group by pv_type, site_type
    )a)t1


left outer join

(-- bounce rate
select pv_type, site_type,
round(sum(case when max_seq_no_in_session=1 then 1 else 0 end)/count(1),4) as bounce_rate

from
    (select distinct pv_type, site_type, uuid, session_id_in_day,max_seq_no_in_session
    from
        (select  pv_type, site_type, uuid, session_id_in_day,
        max(seq_no_in_session) over(partition by uuid, session_id_in_day)as max_seq_no_in_session
        from dw.dw_user_pv_track
        where day = '${hiveconf:y_date}'
        and pv_type in(1,2)
        )t
    )m
group by pv_type, site_type) t2 on (t1.pv_type=t2.pv_type and t1.site_type=t2.site_type)

left outer join

(-- 次日留存率
select
a.pv_type, a.site_type,
round(count(b.uuid)/count(a.uuid),4) as 1_day_retention_rate
from
(select distinct pv_type, site_type,uuid
from dw.dw_user_pv_track
where day = date_sub('${hiveconf:y_date}',1)
and pv_type in (1,2)
and to_date(first_login_time)=day
)a

left outer join

(select distinct pv_type, site_type, uuid
from dw.dw_user_pv_track
where day = '${hiveconf:y_date}'
and pv_type in(1,2)
)b on (a.pv_type=b.pv_type and a.site_type=b.site_type and a.uuid=b.uuid)
group by a.pv_type,a.site_type) t3 on (t1.pv_type=t3.pv_type and t1.site_type=t3.site_type)

left outer join

(-- 七日留存率
select
a.pv_type, a.site_type,
round(count(b.uuid)/count(a.uuid),4) as 7_days_retention_rate
from
(select distinct pv_type, site_type,uuid
from dw.dw_user_pv_track
where day = date_sub('${hiveconf:y_date}',7)
and pv_type in (1,2)
and to_date(first_login_time)=day
)a

left outer join

(select distinct pv_type, site_type, uuid
from dw.dw_user_pv_track
where day = '${hiveconf:y_date}'
and pv_type in(1,2)
)b on (a.pv_type=b.pv_type and a.site_type=b.site_type and a.uuid=b.uuid)
group by a.pv_type,a.site_type)t4 on(t1.pv_type=t4.pv_type and t1.site_type=t4.site_type)













