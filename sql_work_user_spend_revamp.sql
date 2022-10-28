sql_work_user_spend_revamp

## Monthly - Transactions

with vars as (
  select date('2022-08-01','Asia/Jakarta') as start_date
  , date(current_date('Asia/Jakarta')) as end_date
)

select month_date, b.key_merchant, count(distinct a.transaction_reference_id) as transaction_amount
, sum(a.gopay_gtv_amount) as gtv_amount
, sum(a.gopay_burn_amount) as rpl_amount
, safe_divide(coalesce(sum(a.gopay_burn_amount),0),coalesce(sum(gopay_gtv_amount),0)) as rpl_per_gtv_amount
, safe_divide(coalesce(sum(gopay_gtv_amount),0), coalesce(count(distinct transaction_reference_id),0)) as aov_amount
, sum(gopay_revenue_amount) as revenue_amount
, coalesce(sum(gopay_revenue_amount)/sum(gopay_gtv_amount),0) as mdr_amount
from 
`p-gopay-gl-data-mart.gopay_consolidated.detail_gopay_payment_dashboard_foundational_table_daily` a
join `p-gopay-gl-data-mart.gopay_open_loop.external_gopay_user_spend_key_merchant` b
on regexp_contains(a.merchant_name, concat(r'(?i)\b',b.key_merchant, r'\b'))
, vars
where day_date >= vars.start_date
group by 1,2 

## MTD/Daily - Transactions


with vars as (
  select date('2022-08-01','Asia/Jakarta') as start_date
  , date(current_date('Asia/Jakarta')) as end_date
)

select day_date, b.key_merchant, count(distinct a.transaction_reference_id) as transaction_amount
, sum(a.gopay_gtv_amount) as gtv_amount
, sum(a.gopay_burn_amount) as rpl_amount
, safe_divide(coalesce(sum(a.gopay_burn_amount),0),coalesce(sum(gopay_gtv_amount),0)) as rpl_per_gtv_amount
, safe_divide(coalesce(sum(gopay_gtv_amount),0), coalesce(count(distinct transaction_reference_id),0)) as aov_amount
, sum(gopay_revenue_amount) as revenue_amount
, coalesce(sum(gopay_revenue_amount)/sum(gopay_gtv_amount),0) as mdr_amount
from 
`p-gopay-gl-data-mart.gopay_consolidated.detail_gopay_payment_dashboard_foundational_table_daily` a
join `p-gopay-gl-data-mart.gopay_open_loop.external_gopay_user_spend_key_merchant` b
on regexp_contains(a.merchant_name, concat(r'(?i)\b',b.key_merchant, r'\b'))
, vars
where day_date >= vars.start_date
group by 1,2

## Monthly - Users

with vars as (
  select date('2022-08-01','Asia/Jakarta') as start_date
  , date(current_date('Asia/Jakarta')) as end_date
)

select distinct month_date, b.key_merchant, count(distinct a.customer_id)
from `p-gopay-gl-data-mart.gopay_consolidated.detail_gopay_payment_dashboard_foundational_table_daily` a
join `p-gopay-gl-data-mart.gopay_open_loop.external_gopay_user_spend_key_merchant` b
on regexp_contains(a.merchant_name, concat(r'(?i)\b',b.key_merchant, r'\b'))
, vars
where day_date >= vars.start_date
group by 1,2

## MTD/Daily - Users

with vars as (
  select date('2022-08-01','Asia/Jakarta') as start_date
  , date(current_date('Asia/Jakarta')) as end_date
)with vars as (
  select date('2022-08-01','Asia/Jakarta') as start_date
  , date(current_date('Asia/Jakarta')) as end_date
)

select distinct month_date, b.key_merchant, count(distinct a.customer_id)
from `p-gopay-gl-data-mart.gopay_consolidated.detail_gopay_payment_dashboard_foundational_table_daily` a
join `p-gopay-gl-data-mart.gopay_open_loop.external_gopay_user_spend_key_merchant` b
on regexp_contains(a.merchant_name, concat(r'(?i)\b',b.key_merchant, r'\b'))
, vars
where day_date >= vars.start_date
group by 1,2

select distinct day_date, b.key_merchant, count(distinct a.customer_id)
from `p-gopay-gl-data-mart.gopay_consolidated.detail_gopay_payment_dashboard_foundational_table_daily` a
join `p-gopay-gl-data-mart.gopay_open_loop.external_gopay_user_spend_key_merchant` b
on regexp_contains(a.merchant_name, concat(r'(?i)\b',b.key_merchant, r'\b'))
, vars
where day_date >= vars.start_date
group by 1,2



