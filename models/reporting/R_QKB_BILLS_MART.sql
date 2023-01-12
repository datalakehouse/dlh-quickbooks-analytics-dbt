{{ config (
  materialized= 'table',
  schema=var('target_schema', 'QUICKBOOKS'),
  tags= ["staging", "daily"],
  transient=false
)
}}

with bills as (
    select * from {{ ref('W_QKB_BILLS_F') }}
)

, vendors as (
    select * from {{ ref('W_QKB_VENDORS_D')}}
)

, accounts as (
    select * from {{ ref('W_QKB_ACCOUNTS_D')}}
)

select
        --DLHK
         bills.K_BILL_LINE_DLHK
        ,bills.K_BILL_DLHK     
        ,bills.K_SALES_TERM_DLHK
        ,bills.K_VENDOR_DLHK
        ,bills.K_BILL_PAYABLE_ACCOUNT_DLHK
        ,bills.K_BILL_CURRENCY_DLHK
        
        ,bills.K_ITEM_EXPENSE_CUSTOMER_DLHK
        ,bills.K_ITEM_EXPENSE_DLHK
        ,bills.K_ACCOUNT_EXPENSE_CUSTOMER_DLHK
        ,bills.K_ACCOUNT_EXPENSE_DLHK
        ,bills.K_ACCOUNT_DLHK
        ,bills.K_CUSTOMER_DLHK

        --BK
        ,bills.K_BILL_BK
        ,bills.K_SALES_TERM_BK
        ,bills.K_VENDOR_BK
        ,bills.K_BILL_PAYABLE_ACCOUNT_BK
        ,bills.K_BILL_CURRENCY_BK
        ,bills.K_ACCOUNT_BK
        ,bills.K_CUSTOMER_BK
        ,bills.K_ITEM_EXPENSE_TAX_CODE_BK
        ,bills.K_ITEM_EXPENSE_CLASS_BK        
        ,bills.K_ITEM_EXPENSE_CUSTOMER_BK
        ,bills.K_ITEM_EXPENSE_ITEM_BK
        ,bills.K_ACCOUNT_EXPENSE_TAX_CODE_BK
        ,bills.K_ACCOUNT_EXPENSE_CLASS_BK
        ,bills.K_ACCOUNT_EXPENSE_CUSTOMER_BK
        ,bills.K_ACCOUNT_EXPENSE_BK

        --ATTRIBUTES
        ,bills.A_BILL_LINE_INDEX
        ,bills.A_BILLABLE_STATUS        
        ,bills.A_DESCRIPTION
        ,bills.A_DOC_NUMBER
        ,bills.A_DUE_DATE
        ,bills.A_TRANSACTION_DATE
        ,bills.A_INITIAL_PAYMENT_DATE
        ,bills.A_FINAL_PAYMENT_DATE
        ,bills.A_DAYS_BETWEEN_DUE_DATE_AND_NOW
        ,bills.A_BILL_LINE

        ,vendors.A_FULL_NAME AS A_VENDOR_FULL_NAME
        ,vendors.A_GIVEN_NAME AS A_VENDOR_GIVEN_NAME
        ,vendors.A_MIDDLE_NAME AS A_VENDOR_MIDDLE_NAME
        ,vendors.A_FAMILY_NAME AS A_VENDOR_FAMILY_NAME
        ,vendors.A_DISPLAY_NAME AS A_VENDOR_DISPLAY_NAME
        ,vendors.A_TAX_IDENTIFIER AS A_VENDOR_TAX_IDENTIFIER
        ,vendors.A_COMPANY_NAME AS A_CVENDOR_OMPANY_NAME
        ,vendors.A_SUFFIX AS A_VENDOR_SUFFIX
        ,vendors.A_TITLE AS A_VENDOR_TITLE
        ,vendors.A_PRINT_ON_CHECK_NAME AS A_VENDOR_PRINT_ON_CHECK_NAME
        ,vendors.A_EMAIL AS A_VENDOR_EMAIL
        ,vendors.A_WEB_URL AS A_VENDOR_WEB_URL 
        ,vendors.A_ALTERNATE_PHONE_NUMBER AS A_VENDOR_ALTERNATE_PHONE_NUMBER
        ,vendors.A_PRIMARY_PHONE AS A_VENDOR_PRIMARY_PHONE
        ,vendors.A_MOBILE_NUMBER AS A_VENDOR_MOBILE_NUMBER
        ,vendors.A_FAX_NUMBER AS A_VENDOR_FAX_NUMBER
        ,vendors.A_ACCOUND_NUMBER AS A_VENDOR_ACCOUND_NUMBER
        ,vendors.A_SYNC_TOKEN AS A_VENDOR_SYNC_TOKEN
        ,vendors.A_OTHER_CONTACTS AS A_VENDOR_OTHER_CONTACTS
        ,vendors.A_TERM_NAME AS A_VENDOR_TERM_NAME
        ,vendors.A_TERM_TYPE AS A_VENDOR_TERM_TYPE
        ,vendors.A_TERM_DUE_DAYS AS A_VENDOR_TERM_DUE_DAYS

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
        ,vendors.B_VENDOR_1099
        ,vendors.B_ACTIVE AS B_ACTIVE_VENDOR

        --METRICS
        ,bills.M_AMOUNT
        ,bills.M_PAID_AMOUNT
        ,bills.M_UNPAID_AMOUNT
        ,bills.M_ACCOUNT_EXPENSE_TAX_AMOUNT        
        ,bills.M_ITEM_EXPENSE_UNIT_PRICE
        ,bills.M_ITEM_EXPENSE_QUANTITY

        --METADATA
        ,CURRENT_TIMESTAMP as MD_LOAD_DTS
        ,'{{invocation_id}}' AS MD_INTGR_ID

from bills
left join vendors
on bills.K_VENDOR_DLHK = vendors.K_VENDOR_DLHK
left join accounts
on bills.K_ACCOUNT_DLHK = accounts.K_ACCOUNT_DLHK