"""
Synthetic fintech data generator — streaming variant for GB-scale output.

The original generate_data.py builds full Python lists in memory before
writing, which works fine at ~200K rows but will exhaust RAM at tens of
millions of rows. This version writes each record to disk as soon as it's
generated, and only keeps lightweight ID lists in memory (needed so
accounts/transactions/deposits reference real customer_id/account_id
values) rather than full record dicts.

Usage (targets ~13 GB total across all entities):
    python generate_data_large.py --output ../data/raw \
        --customers 1500000 --transactions 60000000 --deposits 6000000

Progress is printed every 500,000 records so you can see it's alive during
a long run.
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
LOAN_STATUSES = ["current", "delinquent", "paid_off", "default"]

PROGRESS_EVERY = 500_000


def random_date(start_days_ago=730, end_days_ago=0):
    start = datetime.now() - timedelta(days=start_days_ago)
    end = datetime.now() - timedelta(days=end_days_ago)
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


def open_writer(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # A large buffer reduces the number of underlying write syscalls, which
    # matters a lot at tens of millions of lines.
    return open(path, "w", buffering=8 * 1024 * 1024)


def generate_customers(path, n):
    """Writes customers to disk as generated; returns the list of
    customer_ids (lightweight — just strings) for downstream sampling."""
    customer_ids = []
    with open_writer(path) as f:
        for i in range(n):
            cid = str(uuid.uuid4())
            customer_ids.append(cid)
            record = {
                "customer_id": cid,
                "full_name": fake.name(),
                "email": fake.email(),
                "segment": random.choice(SEGMENTS),
                "onboarded_date": random_date(1825, 0).strftime("%Y-%m-%d"),
                "country": fake.country_code(),
            }
            f.write(json.dumps(record) + "\n")
            if (i + 1) % PROGRESS_EVERY == 0:
                print(f"  customers: {i + 1:,} / {n:,}")
    print(f"wrote {n:,} customers -> {path}")
    return customer_ids


def generate_accounts(path, customer_ids, accounts_per_customer=(1, 3)):
    account_ids = []
    total = 0
    with open_writer(path) as f:
        for i, cid in enumerate(customer_ids):
            for _ in range(random.randint(*accounts_per_customer)):
                aid = str(uuid.uuid4())
                account_ids.append(aid)
                record = {
                    "account_id": aid,
                    "customer_id": cid,
                    "account_type": random.choice(ACCOUNT_TYPES),
                    "balance": round(random.uniform(0, 50000), 2),
                }
                f.write(json.dumps(record) + "\n")
                total += 1
                if total % PROGRESS_EVERY == 0:
                    print(f"  accounts: {total:,}")
    print(f"wrote {total:,} accounts -> {path}")
    return account_ids


def generate_transactions(path, account_ids, n):
    with open_writer(path) as f:
        for i in range(n):
            record = {
                "txn_id": str(uuid.uuid4()),
                "account_id": random.choice(account_ids),
                "amount": round(random.uniform(-2000, 2000), 2),
                "category": random.choice(TXN_CATEGORIES),
                "txn_ts": random_date(365, 0).isoformat(),
            }
            f.write(json.dumps(record) + "\n")
            if (i + 1) % PROGRESS_EVERY == 0:
                print(f"  transactions: {i + 1:,} / {n:,}")
    print(f"wrote {n:,} transactions -> {path}")


def generate_deposits(path, account_ids, n):
    with open_writer(path) as f:
        for i in range(n):
            record = {
                "deposit_id": str(uuid.uuid4()),
                "account_id": random.choice(account_ids),
                "amount": round(random.uniform(50, 10000), 2),
                "value_date": random_date(365, 0).strftime("%Y-%m-%d"),
            }
            f.write(json.dumps(record) + "\n")
            if (i + 1) % PROGRESS_EVERY == 0:
                print(f"  deposits: {i + 1:,} / {n:,}")
    print(f"wrote {n:,} deposits -> {path}")


def generate_loans(path, customer_ids, fraction=0.3):
    sample_size = int(len(customer_ids) * fraction)
    sampled = random.sample(customer_ids, k=sample_size)
    total = 0
    with open_writer(path) as f:
        for cid in sampled:
            record = {
                "loan_id": str(uuid.uuid4()),
                "customer_id": cid,
                "principal": round(random.uniform(2000, 300000), 2),
                "interest_rate": round(random.uniform(2.5, 18.0), 2),
                "status": random.choice(LOAN_STATUSES),
                "origination_date": random_date(1460, 30).strftime("%Y-%m-%d"),
            }
            f.write(json.dumps(record) + "\n")
            total += 1
            if total % PROGRESS_EVERY == 0:
                print(f"  loans: {total:,} / {sample_size:,}")
    print(f"wrote {total:,} loans -> {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="../data/raw")
    parser.add_argument("--customers", type=int, default=1_500_000)
    parser.add_argument("--transactions", type=int, default=60_000_000)
    parser.add_argument("--deposits", type=int, default=6_000_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    Faker.seed(args.seed)

    print(f"Target: {args.customers:,} customers, {args.transactions:,} "
          f"transactions, {args.deposits:,} deposits")
    print("This will take a while at this scale — progress prints every "
          f"{PROGRESS_EVERY:,} records per file.\n")

    print("Generating customers...")
    customer_ids = generate_customers(
        os.path.join(args.output, "customers/customers.jsonl"), args.customers
    )

    print("\nGenerating accounts...")
    account_ids = generate_accounts(
        os.path.join(args.output, "accounts/accounts.jsonl"), customer_ids
    )

    print("\nGenerating transactions...")
    generate_transactions(
        os.path.join(args.output, "transactions/transactions.jsonl"),
        account_ids, args.transactions,
    )

    print("\nGenerating deposits...")
    generate_deposits(
        os.path.join(args.output, "deposits/deposits.jsonl"),
        account_ids, args.deposits,
    )

    print("\nGenerating loans...")
    generate_loans(
        os.path.join(args.output, "loans/loans.jsonl"), customer_ids
    )

    print("\nDone.")


if __name__ == "__main__":
    main()
