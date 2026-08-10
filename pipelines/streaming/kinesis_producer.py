"""
Streaming demo: simulates live transaction events and publishes them to a
Kinesis Data Stream.

Usage:
    python kinesis_producer.py --stream-name fintech-transactions --rate 5
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

import boto3

TXN_CATEGORIES = [
    "groceries", "utilities", "rent", "dining", "transfer",
    "entertainment", "healthcare", "travel", "salary", "subscription",
]


def make_event():
    return {
        "txn_id": str(uuid.uuid4()),
        "account_id": str(uuid.uuid4()),  # in a real setup, sample from existing accounts
        "amount": round(random.uniform(-500, 500), 2),
        "category": random.choice(TXN_CATEGORIES),
        "txn_ts": datetime.now(timezone.utc).isoformat(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream-name", required=True)
    parser.add_argument("--rate", type=float, default=1.0, help="events per second")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    client = boto3.client("kinesis", region_name=args.region)

    print(f"streaming to '{args.stream_name}' at {args.rate}/s. Ctrl+C to stop.")
    try:
        while True:
            event = make_event()
            client.put_record(
                StreamName=args.stream_name,
                Data=json.dumps(event).encode("utf-8"),
                PartitionKey=event["account_id"],
            )
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        print("stopped")


if __name__ == "__main__":
    main()
