# Data model

The curated layer is modeled as a star schema: `fact_transaction` and
`fact_deposit` as fact tables, with `dim_customer`, `dim_account`, and
`dim_loan` as dimensions.

## ER diagram

```mermaid
erDiagram
  CUSTOMER ||--o{ ACCOUNT : owns
  CUSTOMER ||--o{ LOAN : holds
  ACCOUNT ||--o{ TRANSACTION : has
  ACCOUNT ||--o{ DEPOSIT : receives

  CUSTOMER {
    string customer_id PK
    string full_name
    string segment
    date onboarded_date
    string country
  }
  ACCOUNT {
    string account_id PK
    string customer_id FK
    string account_type
    decimal balance
    date opened_date
  }
  TRANSACTION {
    string txn_id PK
    string account_id FK
    decimal amount
    string category
    timestamp txn_ts
  }
  DEPOSIT {
    string deposit_id PK
    string account_id FK
    decimal amount
    date value_date
  }
  LOAN {
    string loan_id PK
    string customer_id FK
    decimal principal
    decimal interest_rate
    string status
  }
```

## Table grain

| Table | Grain | Type |
|---|---|---|
| `dim_customer` | one row per customer | dimension |
| `dim_account` | one row per account | dimension |
| `dim_loan` | one row per loan | dimension |
| `fact_transaction` | one row per transaction event | fact, partitioned by `txn_date` |
| `fact_deposit` | one row per deposit event | fact |

## Partitioning

`fact_transaction` is partitioned by `txn_date` to keep query costs down in
Athena/Redshift Spectrum and to make the Iceberg table manageable at scale.
