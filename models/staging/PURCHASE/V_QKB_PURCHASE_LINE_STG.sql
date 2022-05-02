{{ config (
  materialized= 'view',
  schema=var('target_schema'),
  tags= ["staging", "daily"]
)
}}

WITH source AS(
    SELECT * FROM  {{source(var('source_schema'),'PURCHASE_LINE')}}
),

accounts AS (
    SELECT * FROM {{ref('W_QKB_ACCOUNTS_D')}}
),

customers AS (
    SELECT * FROM {{ref('W_QKB_CUSTOMERS_D')}}
),

items AS (
    SELECT * FROM {{ref('W_QKB_ITEMS_D')}}
),

rename AS (
SELECT
--DLHK
MD5( TRIM(COALESCE(CONCAT(P.PURCHASE_ID,'-',P.INDEX),'00000000000000000000000000000000')) ) AS K_PURCHASE_LINE_DLHK
,MD5( TRIM(COALESCE(P.PURCHASE_ID,'00000000000000000000000000000000')) ) AS K_PURCHASE_DLHK
,a.K_ACCOUNT_DLHK AS K_ACCOUNT_EXPENSE_DLHK
,cus.K_CUSTOMER_DLHK AS K_ITEM_EXPENSE_CUSTOMER_DLHK
,cus_account.K_CUSTOMER_DLHK AS K_ACCOUNT_EXPENSE_CUSTOMER_DLHK
,I.K_ITEM_DLHK AS K_ITEM_EXPENSE_DLHK
,I.K_EXPENSE_ACCOUNT_DLHK AS K_ITEM_EXPENSE_ACCOUNT_DLHK
--BK
,P.PURCHASE_ID AS K_PURCHASE_BK
,P.ACCOUNT_EXPENSE_ACCOUNT_ID AS K_ACCOUNT_EXPENSE_ACCOUNT_BK
,P.ACCOUNT_EXPENSE_CLASS_ID AS K_ACCOUNT_EXPENSE_CLASS_BK
,P.ACCOUNT_EXPENSE_CUSTOMER_ID AS K_ACCOUNT_EXPENSE_CUSTOMER_BK
,P.ACCOUNT_EXPENSE_TAX_CODE_ID AS K_ACCOUNT_EXPENSE_TAX_CODE_BK
,P.ITEM_EXPENSE_CLASS_ID AS K_ITEM_EXPENSE_CLASS_BK
,P.ITEM_EXPENSE_CUSTOMER_ID AS K_ITEM_EXPENSE_CUSTOMER_BK
,P.ITEM_EXPENSE_ITEM_ID AS K_ITEM_EXPENSE_ITEM_BK
,P.ITEM_EXPENSE_TAX_CODE_ID AS K_ITEM_EXPENSE_TAX_CODE_BK
,I.K_EXPENSE_ACCOUNT_DLHK AS K_ITEM_EXPENSE_ACCOUNT_BK
--ATTRIBUTES
,P.ACCOUNT_EXPENSE_BILLABLE_STATUS AS A_ACCOUNT_EXPENSE_BILLABLE_STATUS
,P.DESCRIPTION AS A_DESCRIPTION
,P.INDEX AS A_INDEX
,P.ITEM_EXPENSE_BILLABLE_STATUS AS A_ITEM_EXPENSE_BILLABLE_STATUS
--METRICS
,P.ACCOUNT_EXPENSE_TAX_AMOUNT::DECIMAL(15,2) AS M_ACCOUNT_EXPENSE_TAX_AMOUNT
,P.AMOUNT::DECIMAL(15,2) AS M_AMOUNT
,P.ITEM_EXPENSE_QUANTITY::DECIMAL(15,2) AS M_ITEM_EXPENSE_QUANTITY
,P.ITEM_EXPENSE_UNIT_PRICE::DECIMAL(15,2) AS M_ITEM_EXPENSE_UNIT_PRICE

 --METADATA
,CURRENT_TIMESTAMP as MD_LOAD_DTS
,'{{invocation_id}}' AS MD_INTGR_ID
FROM
source as P
LEFT JOIN items I on I.K_ITEM_BK = P.ITEM_EXPENSE_ITEM_ID
LEFT JOIN accounts AS a on a.K_ACCOUNT_BK = P.ACCOUNT_EXPENSE_ACCOUNT_ID
LEFT JOIN customers AS cus on cus.K_CUSTOMER_BK = P.ITEM_EXPENSE_CUSTOMER_ID
LEFT JOIN customers AS cus_account on cus_account.K_CUSTOMER_BK = P.ACCOUNT_EXPENSE_CUSTOMER_ID
)

SELECT * FROM rename
