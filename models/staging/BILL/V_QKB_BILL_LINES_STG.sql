{{ config (
  materialized= 'view',
  schema=var('target_schema', 'QUICKBOOKS'),
  tags= ["staging", "daily"]
)
}}

WITH source AS(
    SELECT * FROM  {{source(var('source_schema', 'DEMO_QUICKBOOKS_SANDBOX'),'BILL_LINE')}}
),
items AS (
    SELECT * FROM {{ref('W_QKB_ITEMS_D')}}
),
customers AS (
    SELECT * FROM {{ref('W_QKB_CUSTOMERS_D')}}
),
accounts AS (
    SELECT * FROM {{ref('W_QKB_ACCOUNTS_D')}}
),
rename AS (
SELECT
    --DLHK
    MD5( TRIM(COALESCE(CONCAT(S.BILL_ID,'-',INDEX),'00000000000000000000000000000000')) ) AS K_BILL_LINE_DLHK
    ,MD5( TRIM(COALESCE(S.BILL_ID,'00000000000000000000000000000000')) ) AS K_BILL_DLHK
    ,C.K_CUSTOMER_DLHK AS K_ITEM_EXPENSE_CUSTOMER_DLHK
    ,I.K_ITEM_DLHK AS K_ITEM_EXPENSE_DLHK
    ,I.K_EXPENSE_ACCOUNT_DLHK AS K_ITEM_EXPENSE_ACCOUNT_DLHK
    ,C_ACCOUNT.K_CUSTOMER_DLHK AS K_ACCOUNT_EXPENSE_CUSTOMER_DLHK
    ,A.K_ACCOUNT_DLHK AS K_ACCOUNT_EXPENSE_DLHK
    --BUSINESS KEYS
    ,S.BILL_ID AS K_BILL_BK
    ,NULLIF(S.ITEM_EXPENSE_TAX_CODE_ID,'NON') AS K_ITEM_EXPENSE_TAX_CODE_BK
    ,S.ITEM_EXPENSE_CLASS_ID AS K_ITEM_EXPENSE_CLASS_BK
    ,S.ITEM_EXPENSE_CUSTOMER_ID AS K_ITEM_EXPENSE_CUSTOMER_BK
    ,S.ITEM_EXPENSE_ITEM_ID AS K_ITEM_EXPENSE_ITEM_BK
    ,I.K_EXPENSE_ACCOUNT_DLHK AS K_ITEM_EXPENSE_ACCOUNT_BK
    ,NULLIF(S.ACCOUNT_EXPENSE_TAX_CODE_ID,'NON') AS K_ACCOUNT_EXPENSE_TAX_CODE_BK
    ,S.ACCOUNT_EXPENSE_CLASS_ID AS K_ACCOUNT_EXPENSE_CLASS_BK
    ,S.ACCOUNT_EXPENSE_CUSTOMER_ID AS K_ACCOUNT_EXPENSE_CUSTOMER_BK
    ,S.ACCOUNT_EXPENSE_ACCOUNT_ID AS K_ACCOUNT_EXPENSE_BK
    --ATTRIBUTES
    ,S.INDEX AS A_INDEX    
    ,S.DESCRIPTION AS A_DESCRIPTION
    ,S.ITEM_EXPENSE_BILLABLE_STATUS AS A_ITEM_EXPENSE_BILLABLE_STATUS      
    ,S.ACCOUNT_EXPENSE_BILLABLE_STATUS AS A_ACCOUNT_EXPENSE_BILLABLE_STATUS    
    --METRICS
    ,S.ACCOUNT_EXPENSE_TAX_AMOUNT::decimal(15,2) AS M_ACCOUNT_EXPENSE_TAX_AMOUNT
    ,S.AMOUNT::decimal(15,2) AS M_AMOUNT
    ,S.ITEM_EXPENSE_UNIT_PRICE::decimal(15,2) as M_ITEM_EXPENSE_UNIT_PRICE
    ,S.ITEM_EXPENSE_QUANTITY AS M_ITEM_EXPENSE_QUANTITY

     --METADATA
    ,CURRENT_TIMESTAMP as MD_LOAD_DTS
    ,'{{invocation_id}}' AS MD_INTGR_ID
FROM
    source as S
    LEFT JOIN items I on I.K_ITEM_BK = S.ITEM_EXPENSE_ITEM_ID
    LEFT JOIN customers C on C.K_CUSTOMER_BK = S.ITEM_EXPENSE_CUSTOMER_ID
    LEFT JOIN customers C_ACCOUNT on C_ACCOUNT.K_CUSTOMER_BK = S.ACCOUNT_EXPENSE_CUSTOMER_ID
    LEFT JOIN accounts A on A.K_ACCOUNT_BK = S.ACCOUNT_EXPENSE_ACCOUNT_ID
)

SELECT * FROM rename