{{ config (
  materialized= 'view',
  schema=var('target_schema', 'QUICKBOOKS'),
  tags= ["staging", "daily"]
)
}}

WITH source AS (
  SELECT 
  * 
  FROM  	
    {{source(var('source_schema', 'DEMO_QUICKBOOKS_SANDBOX'),'PAYMENT_LINE')}}
),
rename AS 
(   
SELECT
    --DLHK
    MD5( TRIM(COALESCE(CONCAT(P.PAYMENT_ID,'-',P.PL_INDEX),'00000000000000000000000000000000')) ) AS K_PAYMENT_LINE_DLHK
    ,MD5( TRIM(COALESCE(P.PAYMENT_ID,'00000000000000000000000000000000')) ) AS K_PAYMENT_DLHK
    --BUSINESS_KEYS
    ,P.PAYMENT_ID AS K_PAYMENT_BK    
    ,P.CREDIT_CARD_CHARGE_ID AS K_CREDIT_CARD_CHARGE_BK
    ,P.CREDIT_CARD_CREDIT_ID AS K_CREDIT_CARD_CREDIT_BK
    ,P.CREDIT_MEMO_ID AS K_CREDIT_MEMO_BK
    ,P.DEPOSIT_ID AS K_DEPOSIT_BK
    ,P.EXPENSE_ID AS K_EXPENSE_BK
    ,P.INVOICE_ID AS K_INVOICE_BK
    ,P.JOURNAL_ENTRY_ID AS K_JOURNAL_ENTRY_BK
    ,P.CHECK_ID AS K_CHECK_BK
    --ATTRIBUTES
    ,P.PL_INDEX AS A_INDEX
    --METRIC
    ,P.AMOUNT::DECIMAL(15,2) AS M_AMOUNT
    --METADATA
    ,CURRENT_TIMESTAMP as MD_LOAD_DTS
    ,'{{invocation_id}}' AS MD_INTGR_ID 
  FROM
    source  P
)


SELECT * FROM rename