"""
Batch ETL job for AWS Glue: raw fintech data (JSON, in S3) -> curated
Apache Iceberg tables, registered in the Glue Data Catalog.

This is the Iceberg-native variant of glue_etl_transactions.py. It writes
real Iceberg tables (not plain Parquet) so downstream tools get Iceberg
features: schema evolution, time travel, safe concurrent writes, and
partition evolution.

REQUIRED Glue job setup (Job details tab):
  - Job parameters:
      --input                 s3://<bucket>/raw
      --output                s3://<bucket>/curated   (used as the Iceberg warehouse root)
      --datalake-formats      iceberg
  - This --datalake-formats=iceberg flag is what makes the Iceberg Spark
    runtime jars available to the job; without it, this script will fail
    to find the "iceberg" table source.

The Glue Catalog itself is used as the Iceberg catalog (catalog name:
glue_catalog), so tables created here are immediately visible in Athena,
Redshift Spectrum, and the Glue console — no separate crawler needed for
this table's schema, since Iceberg tables self-describe their schema in
the catalog on write.
"""
import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.conf import SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "input", "output"])

ICEBERG_DATABASE = "fintech_curated_iceberg"

conf = SparkConf()
conf.set(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
)
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.warehouse", args["output"])
conf.set(
    "spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog",
)
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Iceberg tables live in their own catalog database, separate from the
# Parquet tables already registered by the crawler in `fintech_curated`.
spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{ICEBERG_DATABASE}")


def load_raw(spark, input_path):
    customers = spark.read.json(f"{input_path}/customers/customers.jsonl")
    accounts = spark.read.json(f"{input_path}/accounts/accounts.jsonl")
    transactions = spark.read.json(f"{input_path}/transactions/transactions.jsonl")
    deposits = spark.read.json(f"{input_path}/deposits/deposits.jsonl")
    loans = spark.read.json(f"{input_path}/loans/loans.jsonl")
    return customers, accounts, transactions, deposits, loans


def clean_transactions(transactions):
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


def write_iceberg_table(df, table_name, partition_col=None):
    """Creates (or replaces) an Iceberg table in the Glue Catalog."""
    full_name = f"glue_catalog.{ICEBERG_DATABASE}.{table_name}"
    writer = df.writeTo(full_name)
    if partition_col:
        writer = writer.partitionedBy(partition_col)
    writer.using("iceberg").createOrReplace()
    print(f"wrote Iceberg table -> {full_name}")


customers, accounts, transactions, deposits, loans = load_raw(spark, args["input"])

transactions_clean = clean_transactions(transactions)

dim_customer = build_dim_customer(customers)
dim_account = build_dim_account(accounts)
fact_transaction = build_fact_transaction(transactions_clean)

write_iceberg_table(dim_customer, "dim_customer")
write_iceberg_table(dim_account, "dim_account")
write_iceberg_table(fact_transaction, "fact_transaction", partition_col="txn_date")
write_iceberg_table(deposits, "fact_deposit")
write_iceberg_table(loans, "dim_loan")

job.commit()
