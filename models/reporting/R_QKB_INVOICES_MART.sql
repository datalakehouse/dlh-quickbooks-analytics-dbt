{{ config (
  materialized= 'table',
  schema=var('target_schema', 'QUICKBOOKS'),
  tags= ["staging", "daily"],
  transient=false
)
}}

with invoices as (
    select * from {{ ref('W_QKB_INVOICES_F') }}
)

, customers as (
    select * from {{ ref('W_QKB_CUSTOMERS_D')}}
)

, accounts as (
    select * from {{ ref('W_QKB_ACCOUNTS_D')}}
)

select
    --DLHK
     invoices.K_INVOICE_LINE_BUNDLE_DLHK
    ,invoices.K_INVOICE_LINE_DLHK
    ,invoices.K_INVOICE_DLHK
    ,invoices.K_TERM_DLHK
    ,invoices.K_CURRENCY_DLHK
    ,invoices.K_CUSTOMER_DLHK
    ,invoices.K_DEPOSIT_TO_ACCOUNT_DLHK
    ,invoices.K_SALES_ITEM_ITEM_DLHK
    ,invoices.K_SALES_ITEM_CLASS_DLHK
    ,invoices.K_DISCOUNT_ACCOUNT_DLHK
    ,invoices.K_SALES_ITEM_ACCOUNT_DLHK
    ,invoices.K_INVOICE_LINE_BUNDLE_ITEM_DLHK
    ,invoices.K_INVOICE_LINE_BUNDLE_CLASS_DLHK
    ,invoices.K_INVOICE_LINE_BUNDLE_ACCOUNT_DLHK

    --ATTRIBUTES
    ,invoices.A_DOC_NUMBER
    ,invoices.A_TRANSACTION_DATE
    ,invoices.A_BILLING_BCC_EMAIL
    ,invoices.A_TRANSACTION_SOURCE
    ,invoices.A_BILLING_EMAIL
    ,invoices.A_SYNC_TOKEN
    ,invoices.A_CREATED_AT_DTS
    ,invoices.A_UPDATED_AT_DTS
    ,invoices.A_SHIP_DATE
    ,invoices.A_GLOBAL_TAX_CALCULATION
    ,invoices.A_DELIVERY_TYPE
    ,invoices.A_PRINT_STATUS
    ,invoices.A_DELIVERY_TIME
    ,invoices.A_BILLING_CC_EMAIL
    ,invoices.A_CUSTOMER_MEMO
    ,invoices.A_DUE_DATE
    ,invoices.A_EMAIL_STATUS
    ,invoices.A_PRIVATE_NOTE
    ,invoices.A_TRACKING_NUMBER
    ,invoices.A_PAYMENT_DATE
    ,invoices.A_PAYMENT_STATUS
    ,invoices.A_DAYS_BETWEEN_DUE_DATE_AND_NOW
    ,invoices.A_INVOICE_LINE
    ,invoices.A_INVOICE_LINE_DESCRIPTION
    ,invoices.A_DESCRIPTION_SERVICE_DATE
    ,invoices.A_DETAIL_TYPE
    ,invoices.A_INDEX
    ,invoices.A_LINE_NUM
    ,invoices.A_SALES_ITEM_SERVICE_DATE
    ,invoices.A_INVOICE_LINE_BUNDLE_DESCRIPTION
    ,invoices.A_INVOICE_LINE_BUNDLE_LINE_NUM
    ,invoices.A_INVOICE_LINE_BUNDLE_SERVICE_DATE
    ,invoices.M_AMOUNT
    ,invoices.M_PAID_AMOUNT
    ,invoices.M_UNPAID_AMOUNT
    ,invoices.M_BUNDLE_QUANTITY
    ,invoices.M_DISCOUNT_DISCOUNT_PERCENT
    ,invoices.M_SALES_ITEM_DISCOUNT_AMOUNT
    ,invoices.M_SALES_ITEM_DISCOUNT_RATE
    ,invoices.M_SALES_ITEM_QUANTITY
    ,invoices.M_SALES_ITEM_UNIT_PRICE
    ,invoices.M_INVOICE_LINE_BUNDLE_AMOUNT
    ,invoices.M_INVOICE_LINE_BUNDLE_UNIT_PRICE
    ,invoices.M_INVOICE_LINE_BUNDLE_QUANTITY
    ,invoices.M_INVOICE_LINE_DISCOUNT_AMOUNT
    ,invoices.M_INVOICE_LINE_BUNDLE_DISCOUNT_RATE

    ,customers.A_FULL_NAME
    ,customers.A_GIVEN_NAME
    ,customers.A_MIDDLE_NAME
    ,customers.A_FAMILY_NAME
    ,customers.A_DISPLAY_NAME
    ,customers.A_FULLY_QUALIFIED_NAME    
    ,customers.A_COMPANY_NAME    
    ,customers.A_PREFERRED_DELIVERY_METHOD
    ,customers.A_JOB
    ,customers.A_NOTES
    ,customers.A_PRINT_ON_CHECK_NAME
    ,customers.A_EMAIL
    ,customers.A_WEBSITE
    ,customers.A_PHONE_NUMBER
    ,customers.A_ALTERNATE_PHONE_NUMBER    
    ,customers.A_MOBILE_NUMBER
    ,customers.A_FAX_NUMBER

    ,accounts.A_DESCRIPTION AS A_ACCOUNT_DESCRIPTION
    ,accounts.A_FULLY_QUALIFIED_NAME AS A_ACCOUNT_FULLY_QUALIFIED_NAME
    ,accounts.A_ACCOUNT_LEVEL
    ,accounts.A_FIRST_LEVEL_ACCOUNT_NAME
    ,accounts.A_SECOND_LEVEL_ACCOUNT_NAME
    ,accounts.A_THIRD_LEVEL_ACCOUNT_NAME
    ,accounts.A_FOURTH_LEVEL_ACCOUNT_NAME
    ,accounts.A_FIFTH_LEVEL_ACCOUNT_NAME
    ,accounts.A_SIXTH_LEVEL_ACCOUNT_NAME   
    ,accounts.A_ACCOUNT_TYPE
    ,accounts.A_ACCOUNT_SUB_TYPE
    ,accounts.A_ACCOUNT_NAME
    ,accounts.A_ACTIVE AS A_ACTIVE_ACCOUNT
    ,accounts.A_CLASSIFICATION AS A_ACCOUNT_CLASSIFICATION    
    ,accounts.A_ACCOUNT_NUMBER
    ,accounts.A_PARENT_ACCOUNT_NUMBER

    --BOOLEAN
    ,invoices.B_ALLOW_IPNPAYMENT
    ,invoices.B_ALLOW_ONLINE_ACHPAYMENT
    ,invoices.B_APPLY_TAX_AFTER_DISCOUNT
    ,invoices.B_ALLOW_ONLINE_CREDIT_CARD_PAYMENT
    ,invoices.B_ALLOW_ONLINE_PAYMENT
    ,invoices.B_DISCOUNT_PERCENT_BASED

    --METADATA
    ,CURRENT_TIMESTAMP as MD_LOAD_DTS
    ,'{{invocation_id}}' AS MD_INTGR_ID

from invoices
left join customers
on invoices.K_CUSTOMER_DLHK = customers.K_CUSTOMER_DLHK
left join accounts
on invoices.K_SALES_ITEM_ACCOUNT_DLHK = accounts.K_ACCOUNT_DLHK