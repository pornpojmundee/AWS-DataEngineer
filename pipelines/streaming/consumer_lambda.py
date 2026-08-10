"""
Lambda consumer for the Kinesis transaction stream.

Configured as a Kinesis event source for a Lambda function. Aggregates
spend-per-category in a rolling in-memory window per invocation batch and
writes the aggregate to DynamoDB (or S3, if preferred) for downstream
dashboards.

Deploy notes:
- Attach this function as a consumer of the `fintech-transactions` stream.
- Grant it `dynamodb:UpdateItem` on the target table.
- Batch size / window is controlled by the event source mapping config,
  not by this code.
"""
import base64
import json
import os
from collections import defaultdict

import boto3

TABLE_NAME = os.environ.get("AGGREGATES_TABLE", "fintech-spend-aggregates")
dynamodb = boto3.resource("dynamodb")


def handler(event, context):
    table = dynamodb.Table(TABLE_NAME)
    category_totals = defaultdict(float)
    record_count = 0

    for record in event["Records"]:
        payload = base64.b64decode(record["kinesis"]["data"])
        txn = json.loads(payload)
        category_totals[txn["category"]] += txn["amount"]
        record_count += 1

    for category, total in category_totals.items():
        table.update_item(
            Key={"category": category},
            UpdateExpression="ADD running_total :t, txn_count :c",
            ExpressionAttributeValues={":t": total, ":c": 1},
        )

    print(f"processed {record_count} records across {len(category_totals)} categories")
    return {"statusCode": 200, "processed": record_count}
