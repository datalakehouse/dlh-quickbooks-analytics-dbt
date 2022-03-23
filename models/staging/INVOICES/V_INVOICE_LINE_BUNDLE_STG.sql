{{ config (
  materialized= 'view',
  schema=var('target_schema'),
  tags= ["staging", "daily"]
)
}}

WITH source AS (
  SELECT 
  * 
  FROM  	
    {{source(var('source_schema'),'INVOICE_LINE_BUNDLE')}}
),
invoice_line AS (
    SELECT * FROM {{ref('V_INVOICE_LINE_STG')}}
),
accounts AS (
    SELECT * FROM {{ref('W_ACCOUNTS_D')}}
),

class AS (
    SELECT * FROM {{ref('W_CLASS_D')}}
),
item AS (
    SELECT * FROM {{ref('W_ITEMS_D')}}
),
rename AS 
(   
SELECT
    --DLHK
    MD5( TRIM(COALESCE(CONCAT(I.INVOICE_ID,'-',I.INVOICE_LINE_INDEX,'-',I.INDEX),'00000000000000000000000000000000')) ) AS K_INVOICE_LINE_BUNDLE_DLHK
    ,IL.K_INVOICE_LINE_DLHK    
    ,IL.K_INVOICE_DLHK
    ,IT.K_ITEM_DLHK
    ,CLS.K_CLASS_DLHK
    ,A.K_ACCOUNT_DLHK
    --BUSINESS KEYS
    ,I.INVOICE_ID AS K_INVOICE_BK
    ,I.ITEM_ID AS K_ITEM_BK
    ,I.CLASS_ID AS K_CLASS_BK
    ,I.ACCOUNT_ID AS K_ACCOUNT_BK
    ,I.TAX_CODE_ID AS K_TAX_CODE_BK
    --ATTRIBUTES
    ,I.INVOICE_LINE_INDEX AS A_INVOICE_LINE_INDEX    
    ,I.INDEX AS A_INDEX
    ,I.DESCRIPTION AS A_DESCRIPTION
    ,I.LINE_NUM AS A_LINE_NUM
    ,I.SERVICE_DATE AS A_SERVICE_DATE
    --METRICS
    ,I.AMOUNT::DECIMAL(15,2) AS M_AMOUNT
    ,I.UNIT_PRICE::DECIMAL(15,2) AS M_UNIT_PRICE
    ,I.QUANTITY::DECIMAL(15,2) AS M_QUANTITY
    ,I.DISCOUNT_RATE::DECIMAL(15,2) AS M_DISCOUNT_RATE
    ,I.DISCOUNT_AMOUNT::DECIMAL(15,2) AS M_DISCOUNT_AMOUNT    
    --METADATA
    ,CURRENT_TIMESTAMP as MD_LOAD_DTS
    ,'{{invocation_id}}' AS MD_INTGR_ID 
FROM  
    source  I
    LEFT JOIN invoice_line IL ON IL.K_INVOICE_BK = I.INVOICE_ID AND I.INVOICE_LINE_INDEX = IL.A_INDEX
    LEFT JOIN item IT on IT.K_ITEM_DLHK = I.ITEM_ID
    LEFT JOIN class CLS on CLS.K_CLASS_BK = I.CLASS_ID
    LEFT JOIN accounts A ON A.K_ACCOUNT_BK = I.ACCOUNT_ID    
)


SELECT * FROM rename