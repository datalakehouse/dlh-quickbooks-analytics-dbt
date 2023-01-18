{{ config (
  materialized= 'table',
  schema=var('target_schema', 'QUICKBOOKS'),
  tags= ["staging", "daily"],
  transient=false
)
}}

with purchases as (
    select * from {{ ref('W_QKB_PURCHASES_F') }}
)

, vendors as (
    select * from {{ ref('W_QKB_VENDORS_D')}}
)

, accounts as (
    select * from {{ ref('W_QKB_ACCOUNTS_D')}}
)

, customers as (
    select * from {{ ref('W_QKB_CUSTOMERS_D')}}
)

select
        --DLHK
         purchases.K_PURCHASE_LINE_DLHK
        ,purchases.K_PURCHASE_DLHK
        ,purchases.K_PURCHASE_ACCOUNT_DLHK
        ,purchases.K_PURCHASE_CURRENCY_DLHK
        ,purchases.K_PURCHASE_CUSTOMER_DLHK
        ,purchases.K_EMPLOYEE_DLHK     
        ,purchases.K_VENDOR_DLHK
        ,purchases.K_ACCOUNT_EXPENSE_DLHK
        ,purchases.K_ITEM_EXPENSE_CUSTOMER_DLHK
        ,purchases.K_ACCOUNT_EXPENSE_CUSTOMER_DLHK
        ,purchases.K_ITEM_EXPENSE_DLHK
        ,purchases.K_ITEM_EXPENSE_ACCOUNT_DLHK        
        ,purchases.K_ACCOUNT_DLHK
        ,purchases.K_CUSTOMER_DLHK

        --BK 
        ,purchases.K_PURCHASE_BK
        ,purchases.K_CURRENCY_BK
        ,purchases.K_PURCHASE_ACCOUNT_BK
        ,purchases.K_DEPARTMENT_BK
        ,purchases.K_PAYMENT_METHOD_BK
        ,purchases.K_PURCHASE_CUSTOMER_BK
        ,purchases.K_VENDOR_BK
        ,purchases.K_EMPLOYEE_BK
        ,purchases.K_PURCHASE_TAX_CODE_BK
        ,purchases.K_ITEM_EXPENSE_ACCOUNT_BK
        ,purchases.K_ACCOUNT_EXPENSE_ACCOUNT_BK
        ,purchases.K_ACCOUNT_EXPENSE_CLASS_BK
        ,purchases.K_ACCOUNT_EXPENSE_CUSTOMER_BK
        ,purchases.K_ACCOUNT_EXPENSE_TAX_CODE_BK
        ,purchases.K_ITEM_EXPENSE_CLASS_BK
        ,purchases.K_ITEM_EXPENSE_CUSTOMER_BK
        ,purchases.K_ITEM_EXPENSE_ITEM_BK
        ,purchases.K_ITEM_EXPENSE_TAX_CODE_BK

        ,purchases.K_ACCOUNT_BK
        ,purchases.K_CUSTOMER_BK
        
        --ATTRIBUTES
        ,purchases.A_PURCHASE_LINE_INDEX
        ,purchases.A_BILLABLE_STATUS        
        ,purchases.A_DESCRIPTION       
                
        ,purchases.A_PAYMENT_TYPE
        ,purchases.A_PRINT_STATUS
        ,purchases.A_GLOBAL_TAX_CALCULATION
        ,purchases.A_PURCHASE_CREATED_AT_DTS
        ,purchases.A_PURCHASE_UPDATED_AT_DTS        
        ,purchases.A_PURCHASE_TRANSACTION_DATE
        ,purchases.A_PURCHASE_TRANSACTION_SOURCE

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

        --BOOLEAN
        ,vendors.B_VENDOR_1099
        ,vendors.B_ACTIVE AS B_ACTIVE_VENDOR

        --METRICS
        ,purchases.M_AMOUNT
        ,purchases.M_ACCOUNT_EXPENSE_TAX_AMOUNT        
        ,purchases.M_ITEM_EXPENSE_UNIT_PRICE
        ,purchases.M_ITEM_EXPENSE_QUANTITY

         --METADATA
        ,CURRENT_TIMESTAMP as MD_LOAD_DTS
        ,'{{invocation_id}}' AS MD_INTGR_ID

from purchases
left join vendors
on purchases.K_VENDOR_DLHK = vendors.K_VENDOR_DLHK
left join accounts
on purchases.K_ACCOUNT_DLHK = accounts.K_ACCOUNT_DLHK
left join customers
on purchases.K_CUSTOMER_DLHK = customers.K_CUSTOMER_DLHK