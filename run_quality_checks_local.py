"""
Runs the local transformation pipeline and then executes data quality
checks against the resulting curated tables — an end-to-end proof that
the pipeline logic + quality gate work correctly, without needing AWS.

Usage:
    python run_quality_checks_local.py --input data/raw
"""
import argparse
import sys
import os

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "pipelines", "spark_jobs")
)

from pyspark.sql import SparkSession
from etl_transactions import (
    load_raw,
    clean_transactions,
    build_dim_customer,
    build_dim_account,
    build_fact_transaction,
)
from data_quality import run_all_checks, DataQualityError


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/raw")
    args = parser.parse_args()

    spark = SparkSession.builder.master("local[*]").appName("quality-check-run").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Loading raw data from {args.input} ...")
    customers, accounts, transactions, deposits, loans = load_raw(spark, args.input)

    print("Running transformations ...")
    transactions_clean = clean_transactions(transactions)
    dim_customer = build_dim_customer(customers)
    dim_account = build_dim_account(accounts)
    fact_transaction = build_fact_transaction(transactions_clean)

    print(f"  dim_customer: {dim_customer.count():,} rows")
    print(f"  dim_account: {dim_account.count():,} rows")
    print(f"  fact_transaction: {fact_transaction.count():,} rows")
    print(f"  dim_loan: {loans.count():,} rows")

    print("\nRunning data quality checks ...")
    try:
        run_all_checks(dim_customer, dim_account, fact_transaction, loans)
        print("\n✅ ALL DATA QUALITY CHECKS PASSED")
    except DataQualityError as e:
        print(f"\n❌ DATA QUALITY CHECK FAILED: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
