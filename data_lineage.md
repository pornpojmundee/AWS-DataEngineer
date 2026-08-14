# Data lineage

This document traces how data moves through the pipeline, what transforms
each step applies, and where each curated field originates — supporting
audit, debugging, and impact analysis (e.g. "if I change X upstream, what
downstream tables/reports are affected?").

## Pipeline stages

```
[1] Source (synthetic)
        |  Faker-generated, no external upstream system
        v
[2] S3 raw/  (JSON Lines, one file per entity)
        |  Glue ETL job (Spark): clean, dedupe, cast types, derive columns
        v
[3] S3 curated/  (partitioned Parquet)      S3 curated-iceberg/  (Apache Iceberg)
        |  Glue Crawler catalogs schema        |  Iceberg self-registers schema
        v                                       v
[4] Glue Data Catalog
    - database: fintech_curated (Parquet)
    - database: fintech_curated_iceberg (Iceberg)
        |
        v
[5] Consumption
    - Athena (ad hoc SQL)
    - Redshift Spectrum (planned, see README status table)
    - QuickSight dashboards (planned)
```

Each stage is a Glue job or catalog operation; nothing in this pipeline
mutates data in place — every stage reads from one location and writes to
the next, so `raw/` always preserves the original as-generated data.

## Stage-by-stage detail

### [1] → [2]: Generation to raw

**Script**: `data_generator/generate_data.py`
**Output**: `s3://pornpoj-fintech-lakehouse-2026/raw/{customers,accounts,transactions,deposits,loans}/*.jsonl`

No transformation — this is the origin of the data (synthetic, not sourced
from an external system). Referential integrity is enforced at generation
time: `accounts.customer_id` is always sampled from already-generated
`customers`, and `transactions.account_id` / `deposits.account_id` from
already-generated `accounts`.

### [2] → [3]: Raw to curated (ETL)

**Script**: `pipelines/spark_jobs/glue_etl_transactions.py` (Parquet) /
`glue_etl_transactions_iceberg.py` (Iceberg)
**Glue job**: `fintech-etl-job` / `fintech-etl-iceberg-job`

| Curated table | Source table(s) | Transform applied |
|---|---|---|
| `dim_customer` | `customers` | `dropDuplicates(customer_id)` |
| `dim_account` | `accounts` | `dropDuplicates(account_id)` |
| `dim_loan` | `loans` | passthrough (no transform) |
| `fact_deposit` | `deposits` | passthrough (no transform) |
| `fact_transaction` | `transactions` | `dropna(txn_id, account_id, amount, txn_ts)` → `dropDuplicates(txn_id)` → `amount` cast to `double` → `txn_ts` cast to `timestamp` → `txn_date` derived from `txn_ts` (used as the partition key) → `txn_seq_in_account` derived via a window function ordered by `txn_ts` per `account_id` |

### [3] → [4]: Curated to catalog

- **Parquet path**: `fintech-curated-crawler` (Glue Crawler) scans
  `s3://.../curated/`, infers schema from the Parquet file metadata, and
  registers 5 tables in the `fintech_curated` database.
- **Iceberg path**: no crawler needed — the Iceberg writer
  (`writeTo(...).using("iceberg")`) registers schema directly into the
  `fintech_curated_iceberg` database as part of the write itself, and keeps
  a full snapshot history (queryable via `<table>$snapshots`).

### [4] → [5]: Catalog to consumption

- **Athena**: queries either catalog database directly via `AwsDataCatalog`;
  no data movement, reads Parquet/Iceberg files in S3 in place.
- **Redshift Spectrum** (planned): would query the same S3 location through
  an external schema pointing at the Glue Catalog — no data copy required.
- **QuickSight** (planned): would read from Redshift once that layer exists.

## Field-level notes (where meaning could be ambiguous)

See `docs/business_glossary.md` for the technical-field → business-meaning
mapping. This document is about *where a field's value came from and what
was done to it*; the glossary is about *what the field means to the
business*.

| Field | Origin | Notes |
|---|---|---|
| `fact_transaction.txn_date` | Derived, not in source data | `DATE(txn_ts)`, computed during ETL specifically to serve as the Parquet/Iceberg partition key |
| `fact_transaction.txn_seq_in_account` | Derived, not in source data | Row number per `account_id` ordered by `txn_ts`, computed during ETL |
| `dim_account.customer_id` | Copied unchanged from `accounts.customer_id` | Enforced to reference an existing `dim_customer.customer_id` at generation time |
| `dim_loan.*`, `fact_deposit.*` | Copied unchanged from `loans` / `deposits` | No transform applied in the current ETL job (candidates for cleaning if this pipeline used real, less-clean source data) |

## Known gaps

- No column-level lineage tool (e.g. OpenLineage, Marquez) is wired up yet —
  this document is maintained by hand. For a production system handling
  regulated financial data, an automated lineage tool would be the next
  step so lineage stays accurate as the pipeline evolves.
- Lineage for the streaming path (`pipelines/streaming/`) is not covered
  here since that path is not yet deployed (see README status table).
