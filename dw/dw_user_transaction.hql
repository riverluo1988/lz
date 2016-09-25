insert overwrite table dw.dw_user_transaction
partition(year='${hiveconf:year}',month='${hiveconf:month}', day='${hiveconf:y_date}')

select '${hiveconf:y_date}' as date,
t3.user_id,
nvl(t1.day_total,0),
nvl((case when t1.day_total >500 and t1.day_total<=5000 then 'p1'
when t1.day_total>5000 and t1.day_total<=50000 then 'p2'
when t1.day_total>50000 then 'p3'
else 'p0' end),0)as day_prange,

nvl(t1.week_total,0),
nvl((case when t1.week_total >500 and t1.week_total<=5000 then 'p1'
when t1.week_total>5000 and t1.week_total<=50000 then 'p2'
when t1.week_total>50000 then 'p3'
else 'p0' end),0)as week_prange,

nvl(t1.month_total,0),
nvl((case when t1.month_total >500 and t1.month_total<=5000 then 'p1'
when t1.month_total>5000 and month_total<=50000 then 'p2'
when t1.month_total>50000 then 'p3'
else 'p0' end),0)as month_prange,

t2.most_spent_day, round(t2.most_spent_amount,2),

t2.first_spent_day, round(t2.first_spent_amount,2),

t3.top5_item, t4.top5_room

from

(select b.user_id,
concat('{',concat_ws(',',
collect_set(concat_ws(':',cast(item_id as string),cast(cnt as string)))),'}') as top5_item
from
(select user_id,
item_id,
cnt,
row_number() over(partition by user_id order by cnt desc) as rn
from
(select
user_id,item_id,sum(item_num) as cnt
from ods.ods_cdb_trade
where day between '2015-06-01' and '${hiveconf:y_date}'
and trade_status =1
and room_id>0
and product_name='platform'
and item_id >0
group by
user_id,item_id)a)b
where rn<=5
group by b.user_id) t3

left outer join

(select b.user_id,
concat('{',concat_ws(',',
collect_set(concat_ws(':',cast(room_id as string),cast(total_spent_amount as string)))),'}')
as top5_room
from
(select user_id,
room_id,
round(total_spent_amount,2) as total_spent_amount,
row_number() over(partition by user_id order by total_spent_amount desc) as rn
from
(select
user_id,room_id,sum(price) as total_spent_amount
from ods.ods_cdb_trade
where day between '2015-06-01' and '${hiveconf:y_date}'
and trade_status =1
and room_id>0
and product_name='platform'
and item_id >0
group by
user_id,room_id)a)b
where rn<=5
group by b.user_id)t4 on(t3.user_id=t4.user_id)

left outer join

(select
max(struct(day_total, day,user_id)).col1 as most_spent_amount,
max(struct(day_total, day,user_id)).col2 as most_spent_day,
max(struct(day_total, day,user_id)).col3 as user_id,
min(struct(day,day_total,user_id)).col2 as first_spent_amount,
min(struct(day,day_total,user_id)).col1 as first_spent_day,
max(day) as last_spent_day
from
(select day,user_id,sum(price) as day_total
from ods.ods_cdb_trade
where day between '2015-06-01' and '${hiveconf:y_date}'
and trade_status =1
and room_id>0
and product_name='platform'
and item_id >0
group by day, user_id)a
group by user_id)t2 on(t3.user_id=t2.user_id)


left outer join

(select '${hiveconf:y_date}' as date,a.user_id,
round(sum(day_total),2) as day_total,
round(sum(week_total),2) as week_total,
round(sum(month_total),2) as month_total
from
(select
user_id,
sum(price) as day_total,
0 as week_total, 0 as month_total
from ods.ods_cdb_trade
where day = '${hiveconf:y_date}'
and trade_status =1
and room_id>0
and product_name='platform'
and item_id >0
group by user_id

union all

select
user_id,
0 as day_total,
sum(price) as week_total,
0 as month_total
from ods.ods_cdb_trade
where day between date_sub('${hiveconf:y_date}',7) and '${hiveconf:y_date}'
and trade_status =1
and room_id>0
and product_name='platform'
and item_id >0
group by user_id

union all

select
user_id,
0 as day_total,  0 as week_total,
sum(price) as month_total
from ods.ods_cdb_trade
where day between date_sub('${hiveconf:y_date}',30) and '${hiveconf:y_date}'
and trade_status =1
and room_id>0
and product_name='platform'
and item_id >0
group by
user_id)a
group by a.user_id)t1 on(t3.user_id=t1.user_id)



