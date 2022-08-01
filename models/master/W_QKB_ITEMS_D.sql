{{ config (
  materialized= 'table',
  schema=var('target_schema', 'QUICKBOOKS'),
  tags= ["staging", "daily"],
  transient=false
)
}}


SELECT
  *
FROM
  {{ref('V_QKB_ITEMS_STG')}} AS C