# Infrastructure

This project can be deployed with either AWS CDK or Terraform. This folder
is a placeholder for infrastructure-as-code definitions covering:

- S3 buckets: `raw/`, `curated/` with lifecycle policies
- IAM roles for Glue, Lambda, and Airflow (MWAA)
- Glue Database + Crawler
- Kinesis Data Stream
- Lambda function (streaming consumer)
- Redshift Serverless workgroup/namespace

## Suggested next step

Add a `cdk/` (TypeScript or Python) or `terraform/` directory here defining
the above resources, and wire a `deploy` job into
`.github/workflows/ci.yml` to apply changes on merge to `main`.
