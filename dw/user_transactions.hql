select t1.user_id,
t1.day_total,
(case when t1.day_total >500 and t1.day_total<=5000 then 'p1'
when t1.day_total>5000 and t1.day_total<=50000 then 'p2'
when t1.day_total>50000 then 'p3'
else 'p0' end)as day_prange,

t1.week_total,
(case when t1.week_total =500 and t1.week_total<=5000 then 'p1'
when t1.week_total>5000 and t1.week_total<=50000 then 'p2'
when t1.week_total>50000 then 'p3'
else 'p0' end)as week_prange,

t1.month_total,
(case when t1.month_total >500 and t1.month_total<=5000 then 'p1'
when t1.month_total>5000 and month_total<=50000 then 'p2'
when t1.month_total>50000 then 'p3'
else 'p0' end)as month_prange,

t2.most_spent_amount, t2.most_spent_day, t3.top5
from

(select '2016-09-19' as date,a.user_id,
sum(day_total) as day_total,
sum(week_total) as week_total,
sum(month_total) as month_total
from
(select
user_id,
sum(price) as day_total,
0 as week_total, 0 as month_total
from ods.ods_cdb_trade
where day = '2016-09-20'
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
where day between date_sub('2016-09-20',7) and '2016-09-20'
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
where day between date_sub('2016-09-20',30) and '2016-09-20'
and trade_status =1
and room_id>0
and product_name='platform'
and user_id=34368638
and item_id >0
group by
user_id)a
group by a.user_id)t1

left outer join


(select '2016-09-20' as day,
max(struct(day_total, day,user_id)).col1 as most_spent_amount,
max(struct(day_total, day,user_id)).col2 as most_spent_day,
max(struct(day_total, day,user_id)).col3 as user_id,
min(day) as first_pay_day,
max(day) as last_pay_day
from
(select day,user_id,sum(price) as day_total
from ods.ods_cdb_trade
where day between '2015-06-01' and '2016-09-20'
and trade_status =1
and room_id>0
and product_name='platform'
and user_id=34368638
and item_id >0
group by day, user_id)a
group by user_id)t2 on (t1.user_id=t2.user_id and t1.date=t2.day)

left outer join

(select b.user_id,
str_to_map(concat_ws(',',
collect_set(concat_ws(':',cast(item_id as string),cast(cnt as string))))) as top5
from

(select user_id,
item_id,
cnt,
row_number() over(partition by user_id order by cnt desc) as rn
from
(select
user_id,item_id,sum(item_num) as cnt
from ods.ods_cdb_trade
where day between '2015-06-01' and '2016-09-20'
and trade_status =1
and room_id>0
and product_name='platform'
and user_id=34368638
and item_id >0
group by
user_id,item_id)a)b
where rn<=5
group by b.user_id) t3 on t2.user_id=t2.user_id
