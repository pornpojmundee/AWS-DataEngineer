"""
Synthetic fintech data generator.

Generates realistic-looking (but entirely fake) banking data for a data
lakehouse portfolio project: customers, accounts, transactions, deposits,
and loans.

Usage:
    python generate_data.py --output ../data/raw --customers 5000 --transactions 200000
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


def random_date(start_days_ago=730, end_days_ago=0):
    start = datetime.now() - timedelta(days=start_days_ago)
    end = datetime.now() - timedelta(days=end_days_ago)
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


def generate_customers(n):
    customers = []
    for _ in range(n):
        customers.append({
            "customer_id": str(uuid.uuid4()),
            "full_name": fake.name(),
            "email": fake.email(),
            "segment": random.choice(SEGMENTS),
            "onboarded_date": random_date(1825, 0).strftime("%Y-%m-%d"),
            "country": fake.country_code(),
        })
    return customers


def generate_accounts(customers, accounts_per_customer=(1, 3)):
    accounts = []
    for c in customers:
        for _ in range(random.randint(*accounts_per_customer)):
            accounts.append({
                "account_id": str(uuid.uuid4()),
                "customer_id": c["customer_id"],
                "account_type": random.choice(ACCOUNT_TYPES),
                "balance": round(random.uniform(0, 50000), 2),
                "opened_date": c["onboarded_date"],
            })
    return accounts


def generate_transactions(accounts, n):
    account_ids = [a["account_id"] for a in accounts]
    transactions = []
    for _ in range(n):
        amount = round(random.uniform(-2000, 2000), 2)
        transactions.append({
            "txn_id": str(uuid.uuid4()),
            "account_id": random.choice(account_ids),
            "amount": amount,
            "category": random.choice(TXN_CATEGORIES),
            "txn_ts": random_date(365, 0).isoformat(),
        })
    return transactions


def generate_deposits(accounts, n):
    account_ids = [a["account_id"] for a in accounts]
    deposits = []
    for _ in range(n):
        deposits.append({
            "deposit_id": str(uuid.uuid4()),
            "account_id": random.choice(account_ids),
            "amount": round(random.uniform(50, 10000), 2),
            "value_date": random_date(365, 0).strftime("%Y-%m-%d"),
        })
    return deposits


def generate_loans(customers, fraction=0.3):
    loans = []
    sampled = random.sample(customers, k=int(len(customers) * fraction))
    for c in sampled:
        loans.append({
            "loan_id": str(uuid.uuid4()),
            "customer_id": c["customer_id"],
            "principal": round(random.uniform(2000, 300000), 2),
            "interest_rate": round(random.uniform(2.5, 18.0), 2),
            "status": random.choice(LOAN_STATUSES),
            "origination_date": random_date(1460, 30).strftime("%Y-%m-%d"),
        })
    return loans


def write_jsonl(records, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"wrote {len(records):,} records -> {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="../data/raw")
    parser.add_argument("--customers", type=int, default=5000)
    parser.add_argument("--transactions", type=int, default=200000)
    parser.add_argument("--deposits", type=int, default=20000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    Faker.seed(args.seed)

    customers = generate_customers(args.customers)
    accounts = generate_accounts(customers)
    transactions = generate_transactions(accounts, args.transactions)
    deposits = generate_deposits(accounts, args.deposits)
    loans = generate_loans(customers)

    write_jsonl(customers, os.path.join(args.output, "customers/customers.jsonl"))
    write_jsonl(accounts, os.path.join(args.output, "accounts/accounts.jsonl"))
    write_jsonl(transactions, os.path.join(args.output, "transactions/transactions.jsonl"))
    write_jsonl(deposits, os.path.join(args.output, "deposits/deposits.jsonl"))
    write_jsonl(loans, os.path.join(args.output, "loans/loans.jsonl"))


if __name__ == "__main__":
    main()
