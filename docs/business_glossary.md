# Business glossary

Maps technical field names in the curated layer to their business meaning,
so analysts and engineers share a common definition.

| Technical field | Table | Business definition | Owner |
|---|---|---|---|
| `customer_id` | dim_customer | Unique identifier for a customer relationship | Data Engineering |
| `segment` | dim_customer | Customer tier used for pricing and marketing (retail, premium, small_business, student) | Marketing |
| `account_type` | dim_account | Product type of the account (checking, savings, money_market) | Product |
| `balance` | dim_account | Current available balance on the account, in USD | Finance |
| `amount` | fact_transaction | Transaction amount; negative = debit/spend, positive = credit | Finance |
| `category` | fact_transaction | Merchant/spend category assigned to a transaction | Data Engineering |
| `txn_date` | fact_transaction | Partition key derived from `txn_ts`, used for partition pruning | Data Engineering |
| `principal` | dim_loan | Original loan amount disbursed to the customer | Lending |
| `interest_rate` | dim_loan | Annual interest rate (%) applied to the loan | Lending |
| `status` | dim_loan | Current lifecycle state of the loan (current, delinquent, paid_off, default) | Lending |
| `value_date` | fact_deposit | Date the deposit is considered effective for balance purposes | Finance |

## Notes

- All data in this project is synthetic; field definitions mirror real
  banking terminology for portfolio/demonstration purposes only.
- Lineage: raw JSON (S3 `raw/`) → Glue Crawler catalogs schema → Spark ETL
  writes curated Iceberg tables (S3 `curated/`) → Redshift Spectrum / Athena
  query the curated layer.
