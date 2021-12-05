with orders as (
select distinct t2.year_actual, t2.week_of_year, t2.first_day_of_week, t2.last_day_of_week, t1.order_pk, t1.status, t1.order_date
from {{ ref('sat_order_details') }} as t1
left join dbt.d_date as t2
on t1.order_date between t2.first_day_of_week and t2.last_day_of_week
order by 1, 2, 6
)

select year_actual, week_of_year, first_day_of_week, last_day_of_week, status, count(order_pk) as count_ord
from orders
group by year_actual, week_of_year, first_day_of_week, last_day_of_week, status
order by year_actual, week_of_year