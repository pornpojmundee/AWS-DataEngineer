"""
Runs the local transformation pipeline against the deliberately-dirty test
dataset (see data_generator/generate_dirty_test_data.py), and prints a
clear before/after row count so the quality gate's effect is visible —
proof it actually filters bad data rather than just passing because the
main dataset happens to be clean.

Usage:
    python run_quality_checks_dirty_demo.py --input data/dirty_test
"""
import argparse
import sys
import os

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "pipelines", "spark_jobs")
)

from pyspark.sql import SparkSession
from etl_transactions import load_raw, clean_transactions
from data_quality import (
    assert_no_nulls,
    assert_unique,
    assert_referential_integrity,
    DataQualityError,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/dirty_test")
    args = parser.parse_args()

    spark = SparkSession.builder.master("local[*]").appName("dirty-data-demo").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Loading raw (dirty) data from {args.input} ...")
    customers, accounts, transactions_raw, _, _ = load_raw(spark, args.input)

    raw_count = transactions_raw.count()
    print(f"\nRaw transactions.jsonl row count: {raw_count:,}")

    print("\nRunning clean_transactions() (dropna + dropDuplicates + type casts) ...")
    transactions_clean = clean_transactions(transactions_raw)
    clean_count = transactions_clean.count()
    print(f"Row count after cleaning: {clean_count:,}")
    print(f"Rows removed by dropna/dropDuplicates: {raw_count - clean_count:,}")

    dim_account = accounts.dropDuplicates(["account_id"]).select("account_id")

    print("\nRunning individual quality checks against the cleaned data...")

    try:
        assert_no_nulls(
            transactions_clean, ["txn_id", "account_id", "amount", "txn_ts"], "fact_transaction"
        )
        print("  [PASS] no nulls in required fields")
    except DataQualityError as e:
        print(f"  [CHECK CAUGHT AN ISSUE] {e}")

    try:
        assert_unique(transactions_clean, "txn_id", "fact_transaction")
        print("  [PASS] txn_id is unique")
    except DataQualityError as e:
        print(f"  [CHECK CAUGHT AN ISSUE] {e}")

    try:
        assert_referential_integrity(
            transactions_clean, "account_id", dim_account, "account_id", "fact_transaction"
        )
        print("  [PASS] every account_id exists in dim_account (no orphans)")
    except DataQualityError as e:
        print(f"  [CHECK CAUGHT AN ISSUE] {e}")
        print(
            "  -> This is expected: orphan rows were injected on purpose "
            "and clean_transactions() only removes nulls/duplicates, not "
            "orphaned foreign keys. A production pipeline would add an "
            "explicit anti-join filter step before load, or quarantine "
            "these rows rather than fail the whole run."
        )

    spark.stop()


if __name__ == "__main__":
    main()
