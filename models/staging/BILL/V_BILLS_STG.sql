
{{ config (
  materialized= 'view',
  schema=var('target_schema'),
  tags= ["staging", "daily"]
)
}}

WITH BILLS AS (
    SELECT *
    FROM {{ref('V_BILL_HEADER_STG')}}
),

BILL_LINES AS (
    SELECT *
    FROM {{ref('V_BILL_LINES_STG')}}
),
ITEMS AS (
    SELECT *
    FROM {{ref('W_ITEMS_D')}}
),
FINAL AS (
    SELECT
        --DLHK
        BILL_LINES.K_BILL_LINE_DLHK
        ,BILLS.K_BILL_DLHK     
        ,BILLS.K_SALES_TERM_DLHK
        ,BILLS.K_VENDOR_DLHK
        ,BILLS.K_PAYABLE_ACCOUNT_DLHK AS K_BILL_PAYABLE_ACCOUNT_DLHK
        ,BILLS.K_CURRENCY_DLHK AS K_BILL_CURRENCY_DLHK
        
        ,BILL_LINES.K_ITEM_EXPENSE_CUSTOMER_DLHK
        ,BILL_LINES.K_ITEM_EXPENSE_DLHK
        ,BILL_LINES.K_ACCOUNT_EXPENSE_CUSTOMER_DLHK
        ,BILL_LINES.K_ACCOUNT_EXPENSE_DLHK
        ,COALESCE(BILL_LINES.K_ACCOUNT_EXPENSE_DLHK,BILL_LINES.K_ITEM_EXPENSE_ACCOUNT_DLHK) AS K_ACCOUNT_DLHK
        ,COALESCE(BILL_LINES.K_ACCOUNT_EXPENSE_CUSTOMER_DLHK,BILL_LINES.K_ITEM_EXPENSE_CUSTOMER_DLHK) AS K_CUSTOMER_DLHK
        --BK
        ,BILLS.K_BILL_BK
        ,BILLS.K_SALES_TERM_BK
        ,BILLS.K_VENDOR_BK
        ,BILLS.K_PAYABLE_ACCOUNT_BK AS K_BILL_PAYABLE_ACCOUNT_BK
        ,BILLS.K_CURRENCY_BK AS K_BILL_CURRENCY_BK
        ,COALESCE(BILL_LINES.K_ACCOUNT_EXPENSE_BK,BILL_LINES.K_ITEM_EXPENSE_ACCOUNT_BK) AS K_ACCOUNT_BK
        ,COALESCE(BILL_LINES.K_ACCOUNT_EXPENSE_CUSTOMER_BK,BILL_LINES.K_ITEM_EXPENSE_CUSTOMER_BK) AS K_CUSTOMER_BK
        ,BILL_LINES.K_ITEM_EXPENSE_TAX_CODE_BK
        ,BILL_LINES.K_ITEM_EXPENSE_CLASS_BK        
        ,BILL_LINES.K_ITEM_EXPENSE_CUSTOMER_BK
        ,BILL_LINES.K_ITEM_EXPENSE_ITEM_BK
        ,BILL_LINES.K_ACCOUNT_EXPENSE_TAX_CODE_BK
        ,BILL_LINES.K_ACCOUNT_EXPENSE_CLASS_BK
        ,BILL_LINES.K_ACCOUNT_EXPENSE_CUSTOMER_BK
        ,BILL_LINES.K_ACCOUNT_EXPENSE_BK
        --ATTRIBUTES
        ,BILL_LINES.A_INDEX AS A_BILL_LINE_INDEX
        ,COALESCE(BILL_LINES.A_ACCOUNT_EXPENSE_BILLABLE_STATUS,BILL_LINES.A_ITEM_EXPENSE_BILLABLE_STATUS) AS A_BILLABLE_STATUS        
        ,coalesce(BILL_LINES.A_DESCRIPTION, ITEMS.A_ITEM_NAME) as A_DESCRIPTION
        ,BILLS.A_DOC_NUMBER
        ,BILLS.A_TRANSACTION_DATE
        ,BILLS.A_INITIAL_PAYMENT_DATE
        ,BILLS.A_FINAL_PAYMENT_DATE
        --METRICS
        ,BILL_LINES.M_AMOUNT
        ,BILL_LINES.M_ACCOUNT_EXPENSE_TAX_AMOUNT        
        ,BILL_LINES.M_ITEM_EXPENSE_UNIT_PRICE
        ,BILL_LINES.M_ITEM_EXPENSE_QUANTITY

         --METADATA
        ,CURRENT_TIMESTAMP as MD_LOAD_DTS
        ,'{{invocation_id}}' AS MD_INTGR_ID
    FROM BILLS

    INNER JOIN BILL_LINES 
        ON BILLS.K_BILL_DLHK = BILL_LINES.K_BILL_DLHK
    LEFT JOIN ITEMS
        ON ITEMS.K_ITEM_DLHK = BILL_LINES.K_ITEM_EXPENSE_DLHK
)

SELECT *
FROM FINAL