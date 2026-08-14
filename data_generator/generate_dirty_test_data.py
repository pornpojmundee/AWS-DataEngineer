"""
Small, deliberately DIRTY dataset generator — proves the data quality gate
in pipelines/spark_jobs/data_quality.py actually filters bad rows, rather
than just passing because the main dataset happens to already be clean.

This is a separate, small (order of thousands of rows) dataset used only
for this demo. It does NOT touch data/raw or data/raw_large.

Usage:
    python generate_dirty_test_data.py --output ../data/dirty_test

What it injects (each at a configurable rate, default 2%):
    - null values in required fields (txn_id, account_id, amount, txn_ts)
    - duplicate txn_id (exact repeated rows)
    - orphan account_id (transaction references an account_id that does
      not exist in accounts.jsonl) — tests referential integrity checks
"""
import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()

SEGMENTS = ["retail", "premium", "small_business", "student"]
ACCOUNT_TYPES = ["checking", "savings", "money_market"]
TXN_CATEGORIES = [
    "groceries", "utilities", "rent", "dining", "transfer",
    "entertainment", "healthcare", "travel", "salary", "subscription",
]


def random_date(start_days_ago=365, end_days_ago=0):
    start = datetime.now() - timedelta(days=start_days_ago)
    end = datetime.now() - timedelta(days=end_days_ago)
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


def write_jsonl(records, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"wrote {len(records):,} records -> {path}")


def generate_customers(n):
    return [
        {
            "customer_id": str(uuid.uuid4()),
            "full_name": fake.name(),
            "segment": random.choice(SEGMENTS),
            "onboarded_date": random_date(1000, 0).strftime("%Y-%m-%d"),
            "country": fake.country_code(),
        }
        for _ in range(n)
    ]


def generate_accounts(customers, per_customer=(1, 2)):
    accounts = []
    for c in customers:
        for _ in range(random.randint(*per_customer)):
            accounts.append({
                "account_id": str(uuid.uuid4()),
                "customer_id": c["customer_id"],
                "account_type": random.choice(ACCOUNT_TYPES),
                "balance": round(random.uniform(0, 20000), 2),
            })
    return accounts


def generate_dirty_transactions(accounts, n, null_rate, dup_rate, orphan_rate):
    """Generates n clean transactions, then injects bad rows on top so the
    final file is n + injected_count rows — the injected rows are what the
    quality gate is expected to remove."""
    account_ids = [a["account_id"] for a in accounts]
    clean = []
    for _ in range(n):
        clean.append({
            "txn_id": str(uuid.uuid4()),
            "account_id": random.choice(account_ids),
            "amount": round(random.uniform(-1000, 1000), 2),
            "category": random.choice(TXN_CATEGORIES),
            "txn_ts": random_date(365, 0).isoformat(),
        })

    injected = []

    null_count = max(1, int(n * null_rate))
    for _ in range(null_count):
        bad = dict(random.choice(clean))
        field_to_null = random.choice(["txn_id", "account_id", "amount", "txn_ts"])
        bad[field_to_null] = None
        injected.append(bad)

    dup_count = max(1, int(n * dup_rate))
    for _ in range(dup_count):
        injected.append(dict(random.choice(clean)))  # exact duplicate, same txn_id

    orphan_count = max(1, int(n * orphan_rate))
    for _ in range(orphan_count):
        injected.append({
            "txn_id": str(uuid.uuid4()),
            "account_id": str(uuid.uuid4()),  # does not exist in accounts.jsonl
            "amount": round(random.uniform(-1000, 1000), 2),
            "category": random.choice(TXN_CATEGORIES),
            "txn_ts": random_date(365, 0).isoformat(),
        })

    all_rows = clean + injected
    random.shuffle(all_rows)
    return all_rows, {
        "clean_rows": len(clean),
        "injected_null": null_count,
        "injected_duplicate": dup_count,
        "injected_orphan": orphan_count,
        "total_written": len(all_rows),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="../data/dirty_test")
    parser.add_argument("--customers", type=int, default=200)
    parser.add_argument("--transactions", type=int, default=5000)
    parser.add_argument("--null-rate", type=float, default=0.02)
    parser.add_argument("--dup-rate", type=float, default=0.02)
    parser.add_argument("--orphan-rate", type=float, default=0.01)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    random.seed(args.seed)
    Faker.seed(args.seed)

    customers = generate_customers(args.customers)
    accounts = generate_accounts(customers)
    transactions, stats = generate_dirty_transactions(
        accounts, args.transactions, args.null_rate, args.dup_rate, args.orphan_rate
    )

    write_jsonl(customers, os.path.join(args.output, "customers/customers.jsonl"))
    write_jsonl(accounts, os.path.join(args.output, "accounts/accounts.jsonl"))
    write_jsonl(transactions, os.path.join(args.output, "transactions/transactions.jsonl"))
    # deposits/loans not needed for this demo — quality checks focus on
    # fact_transaction's null/duplicate/referential-integrity rules.
    write_jsonl([], os.path.join(args.output, "deposits/deposits.jsonl"))
    write_jsonl([], os.path.join(args.output, "loans/loans.jsonl"))

    print("\n--- Injection summary (what the quality gate should catch) ---")
    for k, v in stats.items():
        print(f"  {k}: {v:,}")
    print(
        f"\nExpected after cleaning: {stats['clean_rows']:,} rows "
        f"(down from {stats['total_written']:,} written)"
    )


if __name__ == "__main__":
    main()
