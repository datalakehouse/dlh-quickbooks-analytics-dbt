{{ config (
  materialized= 'view',
  schema=var('target_schema'),
  tags= ["staging", "daily"]
)
}}

WITH source as 
(
SELECT  *  FROM  {{source(var('source_schema'),'TERM')}}
),
rename AS 
(
SELECT
    MD5( TRIM(COALESCE(ID,'00000000000000000000000000000000')) ) AS K_TERM_DLHK
    ,ID AS K_TERM_BK
    ,DISCOUNT_DAYS AS A_DISCOUNT_DAYS
    ,DISCOUNT_PERCENT AS A_DISCOUNT_PERCENT
    ,NAME AS A_NAME    
    ,TYPE AS A_TYPE
    ,DUE_DAYS AS A_DUE_DAYS
    ,SYNC_TOKEN AS A_SYNC_TOKEN
    ,DAY_OF_MONTH_DUE AS A_DAY_OF_MONTH
    ,DUE_NEXT_MONTH_DAYS AS DUE_NEXT_MONTH_DAYS
    ,DISCOUNT_DAY_OF_MONTH AS A_DISCOUNT_DAY_OF_MONTH
    ,ACTIVE AS B_ACTIVE
FROM
    source
)

SELECT * FROM rename