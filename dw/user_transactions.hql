select t1.*, t2.most_spent, t2.most_spent_day
from

(select '2016-09-19' as date,a.user_id,
sum(day_total),sum(week_total),sum(month_total)
from
(select
user_id,
sum(price) as day_total,
0 as week_total, 0 as month_total
from ods.ods_cdb_trade
where day between '2016-09-19' and '2016-09-19'
and trade_status =1
and room_id>0
and product_name='platform'
and user_id=34368638
and item_id >0
group by user_id

union all

select
user_id,
0 as day_total,
sum(price) as week_total,
0 as month_total
from ods.ods_cdb_trade
where day between date_sub('2016-09-19',7) and '2016-09-19'
and trade_status =1
and room_id>0
and product_name='platform'
and user_id=34368638
and item_id >0
group by user_id

union all

select
user_id,
0 as day_total,  0 as week_total,
sum(price) as month_total
from ods.ods_cdb_trade
where day between add_months('2016-09-19',-1) and '2016-09-19'
and trade_status =1
and room_id>0
and product_name='platform'
and user_id=34368638
and item_id >0
group by
user_id)a
group by a.user_id)t1

left outer join


(select
max(struct(day_total, day,user_id)).col1 as most_spent,
max(struct(day_total, day,user_id)).col2 as most_spent_day,
max(struct(day_total, day,user_id)).col3 as user_id,
max(day) as day
from
(select day,user_id,sum(price) as day_total
from ods.ods_cdb_trade
where day between '2015-06-01' and '2016-09-19'
and trade_status =1
and room_id>0
and product_name='platform'
and user_id=34368638
and item_id >0
group by day, user_id)a
group by user_id)t2 on (t1.user_id=t2.user_id and t1.date=t2.day)









select b.user_id,
str_to_map(concat_ws(',',
collect_set(concat_ws(':',cast(item_id as string),cast(cnt as string)))))
from

(select user_id,
item_id,
cnt,
row_number() over(partition by user_id order by cnt desc) as rn
from
(select
user_id,item_id,sum(item_num) as cnt
from ods.ods_cdb_trade
where day between '2016-06-01' and '2016-09-19'
and trade_status =1
and room_id>0
and product_name='platform'
and user_id=34368638
and item_id >0
group by
user_id,item_id)a)b
where rn<=5
group by b.user_id

