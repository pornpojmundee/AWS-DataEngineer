"""
Unit tests for the Spark transformation logic. Uses a local SparkSession
fixture so these run in CI without any AWS resources.
"""
import pytest
from pyspark.sql import SparkSession

import sys
import os

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "pipelines", "spark_jobs")
)
from etl_transactions import clean_transactions, build_dim_customer  # noqa: E402
from data_quality import (  # noqa: E402
    assert_no_nulls,
    assert_unique,
    DataQualityError,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-fintech-etl")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_clean_transactions_drops_nulls(spark):
    df = spark.createDataFrame(
        [
            ("t1", "a1", 10.0, "groceries", "2026-01-01T00:00:00"),
            (None, "a1", 10.0, "groceries", "2026-01-01T00:00:00"),
        ],
        ["txn_id", "account_id", "amount", "category", "txn_ts"],
    )
    cleaned = clean_transactions(df)
    assert cleaned.count() == 1


def test_clean_transactions_dedupes(spark):
    df = spark.createDataFrame(
        [
            ("t1", "a1", 10.0, "groceries", "2026-01-01T00:00:00"),
            ("t1", "a1", 10.0, "groceries", "2026-01-01T00:00:00"),
        ],
        ["txn_id", "account_id", "amount", "category", "txn_ts"],
    )
    cleaned = clean_transactions(df)
    assert cleaned.count() == 1


def test_dim_customer_deduplicates_on_customer_id(spark):
    df = spark.createDataFrame(
        [
            ("c1", "Alice", "retail", "2020-01-01", "US"),
            ("c1", "Alice", "retail", "2020-01-01", "US"),
        ],
        ["customer_id", "full_name", "segment", "onboarded_date", "country"],
    )
    dim = build_dim_customer(df)
    assert dim.count() == 1


def test_assert_no_nulls_raises_on_null(spark):
    df = spark.createDataFrame([("c1",), (None,)], ["customer_id"])
    with pytest.raises(DataQualityError):
        assert_no_nulls(df, ["customer_id"], "dim_customer")


def test_assert_unique_raises_on_duplicate(spark):
    df = spark.createDataFrame([("c1",), ("c1",)], ["customer_id"])
    with pytest.raises(DataQualityError):
        assert_unique(df, "customer_id", "dim_customer")
