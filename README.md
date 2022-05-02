# Quickbooks package

This dbt package:

*   Contains a DBT dimensional model based on Quickbooks data from [Datalakehouse’s](https://www.datalakehouse.io/) connector.
*   The main use of this package is to provide a stable snowflake dimensional model that will provide useful insights.
    

### Models

The primary ouputs of this package are fact and dimension tables as listed below. There are several intermediate models used to create these models.

|        Type       |        Model       |        Raw tables involved       |
|:----------------:|:----------------:|----------------|
|Dimension| W_QKB_ACCOUNTS_D       | ACCOUNT|
|Dimension| W_QKB_CURRENCY_D         | Manually built |
|Dimension| W_QKB_CLASS_D       | CLASS |
|Dimension| W_QKB_CUSTOMERS_D      | CUSTOMER|
|Dimension| W_QKB_CURRENCY_D         | Manually built |
|Dimension| W_QKB_DATE_D         | Manually built |
|Dimension| W_QKB_EMPLOYEES_D         | EMPLOYEE |
|Dimension| W_QKB_ITEMS_D         | ITEM |
|Dimension| W_QKB_TERM_D         | TERM |
|Fact| W_QKB_VENDORS_D | TERM<br>VENDOR |
|Fact| W_QKB_BILLS_F | BILL<br>BILL_LINE<br>BILL_LINKED_TXN<br>BILL_PAYMENT<br>BILL_PAYMENT_LINE |
|Fact| W_QKB_PAYMENTS_F          | PAYMENT<br>PAYMENT_LINE |
|Fact| W_QKB_INVOICES_F          | INVOICE<br>INVOICE_LINKED_TXN<br>INVOICE_LINE_BUNDLE<br>INVOICE_LINE |
|Fact| W_QKB_PURCHASES_F          | PURCHASE<br>PURCHASE_LINE |

</br>

| ![XWgDcCT.png](https://i.imgur.com/XWgDcCT.png) | 
|:--:| 
| *ERD of Dimensional Model* |

</br>

| ![2INmbKt.jpg](https://i.imgur.com/2INmbKt.jpg) | 
|:--:| 
| *Data Lineage Graph* |

Installation Instructions
-------------------------

Check [dbt Hub](https://hub.getdbt.com) for the latest installation instructions, or [read the docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

Include in your packages.yml

```yaml
packages:
  - package: datalakehouse/dlh-quickbooks-analytics-dbt
    version: [">=0.1.0"]
```

Configuration
-------------

By default, this package uses `DEVELOPER_SANDBOX` as the source database name and `DEMO_QUICKBOOKS` as schema name. If this is not the where your salesforce data is, change ther below [variables](https://docs.getdbt.com/docs/using-variables) configuration on your `dbt_project.yml`:

```yaml
# dbt_project.yml

...

vars:    
    source_database: DEVELOPER_SANDBOX
    source_schema: DEMO_QUICKBOOKS
    target_schema: QUICKBOOKS
```

### Database support

Core:

*   Snowflake
    

### Contributions

Additional contributions to this package are very welcome! Please create issues or open PRs against `main`. Check out [this post](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) on the best workflow for contributing to a package.


*   Fork and :star: this repository :)
*   Check it out and :star: [the datalakehouse core repository](https://github.com/datalakehouse/datalakehouse-core);
