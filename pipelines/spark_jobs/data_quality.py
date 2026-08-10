"""
Data quality checks run on curated tables before they are considered ready
for downstream consumption (Redshift load, reporting).

Kept dependency-light (plain PySpark assertions) so it runs anywhere;
swap in Great Expectations checkpoints for a production setup.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(Exception):
    pass


def assert_no_nulls(df: DataFrame, columns: list[str], table_name: str):
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            raise DataQualityError(
                f"[{table_name}] column '{col}' has {null_count} null values"
            )


def assert_unique(df: DataFrame, column: str, table_name: str):
    total = df.count()
    distinct = df.select(column).distinct().count()
    if total != distinct:
        raise DataQualityError(
            f"[{table_name}] column '{column}' is not unique: "
            f"{total} rows vs {distinct} distinct values"
        )


def assert_referential_integrity(
    child_df: DataFrame, child_key: str, parent_df: DataFrame, parent_key: str, table_name: str
):
    orphans = child_df.join(
        parent_df.select(parent_key), child_df[child_key] == parent_df[parent_key], "left_anti"
    ).count()
    if orphans > 0:
        raise DataQualityError(
            f"[{table_name}] {orphans} rows in '{child_key}' have no matching '{parent_key}'"
        )


def assert_value_range(df: DataFrame, column: str, min_value: float, max_value: float, table_name: str):
    out_of_range = df.filter(
        (F.col(column) < min_value) | (F.col(column) > max_value)
    ).count()
    if out_of_range > 0:
        raise DataQualityError(
            f"[{table_name}] column '{column}' has {out_of_range} values outside "
            f"[{min_value}, {max_value}]"
        )


def run_all_checks(dim_customer, dim_account, fact_transaction, dim_loan):
    assert_no_nulls(dim_customer, ["customer_id", "full_name"], "dim_customer")
    assert_unique(dim_customer, "customer_id", "dim_customer")

    assert_no_nulls(dim_account, ["account_id", "customer_id"], "dim_account")
    assert_unique(dim_account, "account_id", "dim_account")
    assert_referential_integrity(
        dim_account, "customer_id", dim_customer, "customer_id", "dim_account"
    )

    assert_no_nulls(fact_transaction, ["txn_id", "account_id", "amount"], "fact_transaction")
    assert_unique(fact_transaction, "txn_id", "fact_transaction")
    assert_referential_integrity(
        fact_transaction, "account_id", dim_account, "account_id", "fact_transaction"
    )

    assert_value_range(dim_loan, "interest_rate", 0, 40, "dim_loan")

    print("all data quality checks passed")
