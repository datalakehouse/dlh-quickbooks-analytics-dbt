{{ config (
  materialized= 'view',
  schema=var('target_schema', 'QUICKBOOKS'),
  tags= ["staging", "daily"]
)
}}
with invoices as (
    select * from {{ ref('W_QKB_INVOICES_F') }}
)

select 
round((DIV0(sum(M_UNPAID_AMOUNT),sum(case when A_TRANSACTION_DATE >= dateadd(day, -364, current_date) then M_AMOUNT else 0 end)) * 365),2) as days_sales_outstanding
from invoices
