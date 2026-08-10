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
│   ├── spark_jobs/       # PySpark batch ETL (raw -> Iceberg curated)
│   └── streaming/        # Kinesis producer + Lambda consumer demo
├── airflow/dags/         # Airflow DAG orchestrating the pipeline
├── infra/                 # infrastructure as code notes (CDK/Terraform)
├── tests/                 # pytest unit tests for transformations
├── docs/
│   ├── er_diagram.md      # data model + ER diagram (mermaid)
│   └── business_glossary.md
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

## License

MIT
