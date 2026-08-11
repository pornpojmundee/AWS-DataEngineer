"""
Batch ETL job for AWS Glue: raw fintech data (JSON, in S3) -> curated Parquet
tables (in S3), partitioned for efficient Athena/Redshift Spectrum queries.

This is the Glue-runnable variant of etl_transactions.py: it writes plain
partitioned Parquet instead of Apache Iceberg tables, so it runs on a
default Glue job with no extra Iceberg configuration. Once this pipeline
is verified working end-to-end, swap the writer for Iceberg (see
etl_transactions.py's write_iceberg comment) by enabling Iceberg on the
Glue job (--datalake-formats=iceberg) and using df.writeTo(...).

Glue job parameters expected (Job parameters tab in the console):
    --input   s3://<bucket>/raw
    --output  s3://<bucket>/curated
"""
import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "input", "output"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


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


def write_table(df, table_name, output_path, partition_col=None):
    writer = df.write.mode("overwrite").format("parquet")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.save(f"{output_path}/{table_name}")
    print(f"wrote curated table -> {output_path}/{table_name}")


customers, accounts, transactions, deposits, loans = load_raw(spark, args["input"])

transactions_clean = clean_transactions(transactions)

dim_customer = build_dim_customer(customers)
dim_account = build_dim_account(accounts)
fact_transaction = build_fact_transaction(transactions_clean)

write_table(dim_customer, "dim_customer", args["output"])
write_table(dim_account, "dim_account", args["output"])
write_table(fact_transaction, "fact_transaction", args["output"], partition_col="txn_date")
write_table(deposits, "fact_deposit", args["output"])
write_table(loans, "dim_loan", args["output"])

job.commit()
