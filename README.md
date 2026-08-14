# Fintech data lakehouse pipeline

An end-to-end data engineering project simulating a fintech data platform on AWS.
Built to demonstrate batch + streaming pipeline engineering, lakehouse table
management (Apache Iceberg), data modeling, data quality, orchestration, and
CI/CD — using synthetic banking data (customers, accounts, transactions, loans,
deposits).

## Why this project

This project was built to mirror the responsibilities of a modern Data Engineer
role in financial services:

- Batch and streaming pipeline engineering (Spark + Kinesis)
- Data modeling for analytics (star schema + ER diagram)
- Data catalog and business glossary maintenance
- SDLC best practices: Git, automated tests, CI/CD, Airflow DAGs
- Financial domain modeling: customer profiles, spending, lending, deposits
- Reporting from a lakehouse (Iceberg + Redshift)
- Data quality checks

## Architecture

```
                 ┌──────────────────┐
 synthetic data  │   S3  raw/       │
 (Faker) ───────▶│  (JSON/CSV)      │
                 └────────┬─────────┘
                          │  Glue Crawler
                          ▼
                 ┌──────────────────┐
                 │  Glue Catalog     │
                 └────────┬─────────┘
                          │  PySpark ETL (Glue/EMR Serverless)
                          ▼
                 ┌──────────────────┐
                 │ S3 curated/       │
                 │ Apache Iceberg     │
                 │ tables (partitioned)│
                 └────────┬─────────┘
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
      ┌───────────────┐      ┌────────────────┐
      │ Athena queries │      │ Redshift        │
      │ (ad hoc)       │      │ Serverless / BI │
      └───────────────┘      └────────┬────────┘
                                       ▼
                              ┌────────────────┐
                              │ QuickSight       │
                              │ dashboards       │
                              └────────────────┘

  Streaming path (demo):
  transaction events → Kinesis Data Stream → Lambda consumer → aggregated
  metrics in DynamoDB/S3

  Orchestration: Airflow DAG runs ingest → transform → data quality → load
  CI/CD: GitHub Actions runs tests + lint on every push
```

## Repo structure

```
fintech-data-lakehouse/
├── data_generator/       # synthetic fintech data generator (Faker)
├── pipelines/
│   ├── spark_jobs/       # PySpark batch ETL: Parquet variant + Iceberg variant (Glue-ready)
│   └── streaming/        # Kinesis producer + Lambda consumer demo
├── airflow/
│   ├── dags/              # Airflow DAG orchestrating the pipeline
│   └── Dockerfile          # custom Airflow image with Java + PySpark
├── infra/                 # infrastructure as code notes (CDK/Terraform)
├── tests/                 # pytest unit tests for transformations
├── docs/
│   ├── er_diagram.md      # data model + ER diagram (mermaid)
│   ├── business_glossary.md
│   └── data_lineage.md    # field-level lineage: source -> raw -> curated -> consumption
└── .github/workflows/ci.yml
```

## Data domains

| Entity | Description |
|---|---|
| `customer` | Customer profile, segment, onboarding date |
| `account` | Bank account owned by a customer (checking/savings) |
| `transaction` | Spending/transfer activity on an account |
| `deposit` | Deposit events into an account |
| `loan` | Lending product held by a customer |

## Tech stack

| Layer | Tool |
|---|---|
| Storage | Amazon S3, Apache Iceberg |
| Processing (batch) | Apache Spark (AWS Glue / EMR Serverless) |
| Processing (streaming) | Amazon Kinesis, AWS Lambda |
| Catalog | AWS Glue Data Catalog, AWS Lake Formation |
| Warehouse / BI | Amazon Redshift Serverless, Amazon QuickSight |
| Orchestration | Apache Airflow |
| Data quality | Great Expectations |
| CI/CD | GitHub Actions, pytest |

## Getting started

```bash
# 1. Generate synthetic data
cd data_generator
pip install -r requirements.txt
python generate_data.py --output ../data/raw --customers 5000 --transactions 200000

# 2. Run the Spark ETL job locally (requires pyspark)
cd ../pipelines/spark_jobs
python etl_transactions.py --input ../../data/raw --output ../../data/curated

# 3. Run tests
cd ../../
pytest tests/

# 4. Run Airflow locally (optional, requires Docker)
cd airflow
docker compose up
```

## Status

This is a portfolio / learning project built to practice the skills required
for a financial-services Data Engineer role. Data is entirely synthetic.

### What's actually deployed on AWS (verified, not just designed)

| Component | Status | Evidence |
|---|---|---|
| S3 data lake (`raw/`, `curated/`) | ✅ Live | `s3://pornpoj-fintech-lakehouse-2026/` |
| IAM user + role (least-privilege) | ✅ Live | `data-engineer-dev` user, `AWSGlueServiceRole-fintech` role |
| Spark batch ETL on AWS Glue | ✅ Ran successfully | Glue job `fintech-etl-job`, Glue 5.1, produced partitioned Parquet curated tables |
| Glue Data Catalog | ✅ Live | Database `fintech_curated`, 5 tables cataloged via crawler |
| Athena querying | ✅ Verified | Ad-hoc SQL queries against curated tables ran successfully |
| Data quality checks | ✅ Verified locally | `run_quality_checks_local.py` — null, duplicate, and referential-integrity checks passed against 200k+ transactions |
| CI (GitHub Actions + pytest) | ✅ Live | Runs on every push |
| Apache Iceberg tables | ✅ Ran successfully | Separate Glue job `fintech-etl-iceberg-job` writes real Iceberg tables (catalog `fintech_curated_iceberg`) via Glue Catalog integration (`--datalake-formats=iceberg`); verified with Athena queries and Iceberg's `$snapshots` metadata table showing a real snapshot/overwrite history |
| Kinesis + Lambda streaming | 🔲 Code written, not deployed | `pipelines/streaming/` has a working producer/consumer pair; not yet wired to a live Kinesis stream |
| Redshift Serverless | 🔲 Designed, not deployed | Blocked on AWS account payment verification; schema/query plan described above |
| Apache Flink | 🔲 Not implemented | Kinesis + Lambda used instead as a lighter-weight stand-in for real-time processing |
| QuickSight dashboards | 🔲 Not built | Depends on Redshift being live |
| Airflow orchestration | 🟡 Partially verified locally | DAG (`fintech_pipeline_dag.py`) runs in Docker Compose against a custom image with Java + PySpark added (see `airflow/Dockerfile`); the DAG, its 4-task dependency chain, and scheduling are confirmed working in the Airflow UI, and the first task (`generate_raw_data`) has run successfully multiple times. The downstream Spark task has not yet completed a clean end-to-end run — local container restarts repeatedly orphaned in-progress task instances rather than any issue in the pipeline code itself (the same transformation logic already runs successfully on AWS Glue, see above) |

## License

MIT
