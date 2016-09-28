create table if not exists rpt.rpt_user_cnt_by_category(
date                string,
create_time         string,
type                string comment '类型',
cid                 bigint comment '分类id',
cdomain             string comment '分类域名',
name                string comment '分类名',
pv                  bigint,
uv                  bigint comment'用户数')
comment 'qqbrowser,tgp,gamersky分类用户量'
;

insert into table rpt.rpt_user_cnt_by_category
select '${hiveconf:y_date}'as date,
unix_timestamp() as create_time,
type, cid, cdomain, name, pv, uv
from
(select a.type, c.id as cid, c.domain as cdomain, c.name,
count(1) as pv, count(distinct uuid) as uv
from
(select sbt, uuid, 'qqbrowser'as type
from ods.ods_web_log_page_view
where day = '${hiveconf:y_date}'
and length(sbt)>1
and (source_url='' or source_url rlike 'video.browser.qq.com')
and url rlike 'from=qqbrowser'

union all

select sbt, uuid, 'tgp'as type
from ods.ods_web_log_page_view
where day = '${hiveconf:y_date}'
and length(sbt)>1
and tid rlike '400001-'

union all

select sbt, uuid, 'gamersky'as type
from ods.ods_web_log_page_view
where day = '${hiveconf:y_date}'
and length(sbt)>1
and source_url rlike 'gamersky.com'
and url rlike 'longzhu.com')a
left outer join dim.dim_room b on a.sbt=b.room_id
left outer join dim.dim_category c on c.id=b.category_id
group by a.type, c.id, c.domain,c.name)all