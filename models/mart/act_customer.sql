with time1 as (
select coalesce(t1.customer_pk, t2.customer_pk) as pk,  MAX(t1.effective_from) as t1_dttm, MAX(t2.effective_from) as t2_dttm
from {{ ref('sat_customer_details') }} as t1
full join {{ ref('sat_customer_crm_details') }} as t2
on t1.customer_pk=t2.customer_pk
where t1.effective_from < '{{ var('ch_dttm') }}'::timestamp
and t2.effective_from < '{{ var('ch_dttm') }}'::timestamp
group by pk
)

select COALESCE(t1.customer_pk, t2.customer_pk) as customer_pk, t1.first_name, t1.last_name, t1.email, t2.country,
t2.age, t3.t1_dttm as effective_from_t1, t3.t2_dttm as effective_from_t2, t1.load_date as load_date_t1, t2.load_date as load_date_t2
from dbt.sat_customer_details as t1 
full join dbt.sat_customer_crm_details as t2
on t1.customer_pk=t2.customer_pk 
inner join time1 as t3
on COALESCE(t1.customer_pk, t2.customer_pk) = t3.pk and t1.effective_from = t1_dttm and t2.effective_from = t2_dttm