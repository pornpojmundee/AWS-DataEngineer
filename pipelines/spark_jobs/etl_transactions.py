"""
Batch ETL job: raw fintech data (JSON) -> curated Apache Iceberg tables.

Intended to run on AWS Glue (Spark 3.x with Iceberg runtime) or EMR
Serverless. Can also run locally against a local Iceberg catalog for
development, as long as the Iceberg Spark runtime jar is on the classpath.

Usage (local dev):
    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
        etl_transactions.py --input ../../data/raw --output ../../data/curated
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_spark(app_name="fintech-etl"):
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.local.type", "hadoop")
        .getOrCreate()
    )


def load_raw(spark, input_path):
    customers = spark.read.json(f"{input_path}/customers/customers.jsonl")
    accounts = spark.read.json(f"{input_path}/accounts/accounts.jsonl")
    transactions = spark.read.json(f"{input_path}/transactions/transactions.jsonl")
    deposits = spark.read.json(f"{input_path}/deposits/deposits.jsonl")
    loans = spark.read.json(f"{input_path}/loans/loans.jsonl")
    return customers, accounts, transactions, deposits, loans


def clean_transactions(transactions):
    """Basic cleaning: drop nulls on key columns, dedupe on txn_id, cast types,
    derive a date partition column."""
    return (
        transactions.dropna(subset=["txn_id", "account_id", "amount", "txn_ts"])
        .dropDuplicates(["txn_id"])
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("txn_ts", F.to_timestamp("txn_ts"))
        .withColumn("txn_date", F.to_date("txn_ts"))
    )


def build_dim_customer(customers):
    return customers.dropDuplicates(["customer_id"]).select(
        "customer_id", "full_name", "segment", "onboarded_date", "country"
    )


def build_dim_account(accounts):
    return accounts.dropDuplicates(["account_id"]).select(
        "account_id", "customer_id", "account_type", "balance", "opened_date"
    )


def build_fact_transaction(transactions_clean):
    window = Window.partitionBy("account_id").orderBy("txn_ts")
    return transactions_clean.withColumn(
        "txn_seq_in_account", F.row_number().over(window)
    )


def write_iceberg(df, table_name, output_path, partition_col=None):
    """Writes a DataFrame as an Iceberg table.

    In this repo's local/demo mode we write partitioned Parquet directly
    (readable by Athena/Redshift Spectrum) so the pipeline is runnable
    without a live Iceberg catalog. On Glue/EMR with a configured Glue
    Catalog, replace this with `df.writeTo(f"glue_catalog.curated.{table_name}").createOrReplace()`.
    """
    writer = df.write.mode("overwrite").format("parquet")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.save(f"{output_path}/{table_name}")
    print(f"wrote curated table -> {output_path}/{table_name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = build_spark()
    customers, accounts, transactions, deposits, loans = load_raw(spark, args.input)

    transactions_clean = clean_transactions(transactions)

    dim_customer = build_dim_customer(customers)
    dim_account = build_dim_account(accounts)
    fact_transaction = build_fact_transaction(transactions_clean)

    write_iceberg(dim_customer, "dim_customer", args.output)
    write_iceberg(dim_account, "dim_account", args.output)
    write_iceberg(fact_transaction, "fact_transaction", args.output, partition_col="txn_date")
    write_iceberg(deposits, "fact_deposit", args.output)
    write_iceberg(loans, "dim_loan", args.output)

    spark.stop()


if __name__ == "__main__":
    main()
