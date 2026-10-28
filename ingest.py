
import json
import os
from datetime import timezone

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

load_dotenv()

# -----------------------------
# MongoDB connection
# -----------------------------
MONGODB_URI = os.getenv("MONGODB_URI")
DATABASE_NAME = os.getenv("MONGODB_DB", "sentinel")
CSV_PATH = "data/part-00028.csv"

if not MONGODB_URI:
    raise ValueError("MONGODB_URI is missing from your .env file.")

client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10000)
client.admin.command("ping")

db = client[DATABASE_NAME]
customers = db["customers"]
transactions = db["transactions"]


# -----------------------------
# Helper functions
# -----------------------------
def clean_value(value):
    """Convert pandas missing values into Python None."""
    if pd.isna(value):
        return None
    return value


def parse_location(value):
    """
    Convert JSON location text such as
    {"city":"Pune","lat":18.6,"lon":73.8}
    into a MongoDB embedded document.
    """
    value = clean_value(value)

    if value is None:
        return None

    if isinstance(value, dict):
        return value

    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {"raw": str(value)}


def parse_timestamp(value):
    """Convert the CSV timestamp to a timezone-aware UTC datetime."""
    timestamp = pd.to_datetime(value, utc=True, errors="raise")
    return timestamp.to_pydatetime()


def to_int(value):
    value = clean_value(value)
    return int(value) if value is not None else None


def to_float(value):
    value = clean_value(value)
    return float(value) if value is not None else None


# -----------------------------
# Ingestion function
# -----------------------------
def ingest_csv(csv_path=CSV_PATH):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"CSV file not found: {csv_path}. "
            "Check the file path."
        )

    df = pd.read_csv(csv_path)

    # Actual columns from part-00028.csv
    required_columns = {
        "transaction_id",
        "customer_id",
        "merchant_id",
        "device_id",
        "amount_paise",
        "transaction_amount",
        "payment_channel",
        "merchant_category",
        "transaction_time",
        "customer_age",
        "customer_location",
        "merchant_location",
        "account_age",
        "previous_transactions",
        "average_transaction_value",
        "failed_transactions",
        "split",
        "is_synthetic",
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"CSV is missing columns: {sorted(missing_columns)}"
        )

    if df["transaction_id"].isna().any():
        raise ValueError("CSV contains missing transaction_id values.")

    if df["transaction_id"].duplicated().any():
        raise ValueError(
            "CSV contains duplicate transaction_id values. "
            "Resolve duplicates before ingestion."
        )

    # Convert locations into embedded documents.
    df["customer_location"] = df["customer_location"].apply(
        parse_location
    )
    df["merchant_location"] = df["merchant_location"].apply(
        parse_location
    )

    # Convert timestamps into UTC datetime objects.
    df["transaction_time"] = df["transaction_time"].apply(
        parse_timestamp
    )

    # -----------------------------
    # Prepare customer documents
    # -----------------------------
    customer_documents = {}

    for row in df.to_dict(orient="records"):
        customer_id = str(row["customer_id"])

        if customer_id not in customer_documents:
            customer_documents[customer_id] = {
                "customer_id": customer_id,
                "customer_age": to_int(row["customer_age"]),
                "account_age": to_int(row["account_age"]),
                "customer_location": row["customer_location"],
                "devices": set(),
            }

        customer = customer_documents[customer_id]

        # Keep available values if another row has a missing value.
        if customer["customer_location"] is None:
            customer["customer_location"] = row["customer_location"]

        device_id = clean_value(row["device_id"])
        if device_id is not None:
            customer["devices"].add(str(device_id))

    customer_operations = []

    for customer_id, customer in customer_documents.items():
        update_fields = {
            "customer_age": customer["customer_age"],
            "account_age": customer["account_age"],
        }

        if customer["customer_location"] is not None:
            update_fields["customer_location"] = (
                customer["customer_location"]
            )

        customer_operations.append(
            UpdateOne(
                {"customer_id": customer_id},
                {
                    "$set": update_fields,
                    "$addToSet": {
                        "devices": {
                            "$each": sorted(customer["devices"])
                        }
                    },
                },
                upsert=True,
            )
        )

    if customer_operations:
        for start in range(0, len(customer_operations), 1000):
            customers.bulk_write(
                customer_operations[start:start + 1000],
                ordered=False,
            )

    # -----------------------------
    # Prepare transaction documents
    # -----------------------------
    transaction_documents = []

    for row in df.to_dict(orient="records"):
        transaction_documents.append({
            "transaction_id": str(row["transaction_id"]),
            "customer_id": str(row["customer_id"]),
            "merchant_id": str(row["merchant_id"]),
            "device_id": clean_value(row["device_id"]),
            "amount_paise": to_int(row["amount_paise"]),
            "transaction_amount": to_float(
                row["transaction_amount"]
            ),
            "payment_channel": clean_value(row["payment_channel"]),
            "merchant_category": clean_value(
                row["merchant_category"]
            ),
            "transaction_time": row["transaction_time"],
            "customer_age": to_int(row["customer_age"]),
            "customer_location": row["customer_location"],
            "merchant_location": row["merchant_location"],
            "account_age": to_int(row["account_age"]),
            "previous_transactions": to_int(
                row["previous_transactions"]
            ),
            "average_transaction_value": to_float(
                row["average_transaction_value"]
            ),
            "failed_transactions": to_int(
                row["failed_transactions"]
            ),
            "split": clean_value(row["split"]),
            "is_synthetic": bool(row["is_synthetic"]),
        })

    # Unique index protects transaction identity.
    transactions.create_index(
        [("transaction_id", 1)],
        unique=True,
        name="idx_transaction_id_unique",
    )

    # Upsert makes rerunning ingestion safe for existing IDs.
    for start in range(0, len(transaction_documents), 1000):
        batch = transaction_documents[start:start + 1000]

        operations = [
            UpdateOne(
                {"transaction_id": doc["transaction_id"]},
                {"$set": doc},
                upsert=True,
            )
            for doc in batch
        ]

        transactions.bulk_write(operations, ordered=False)

    # -----------------------------
    # Report results
    # -----------------------------
    print("\nIngestion completed successfully.")
    print("CSV rows read:", len(df))
    print("Unique customers in CSV:", len(customer_documents))
    print("Customers in MongoDB:", customers.count_documents({}))
    print("Transactions in MongoDB:", transactions.count_documents({}))
    print("Rows marked synthetic:", int(df["is_synthetic"].sum()))
    print("Split distribution:")
    print(df["split"].value_counts(dropna=False).to_string())

    print("\nSample transaction:")
    print(
        transactions.find_one(
            {},
            {"_id": 0}
        )
    )


if __name__ == "__main__":
    try:
        ingest_csv()
    finally:
        client.close()
