
import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, DESCENDING

# --------------------------------------------------
# 1. MongoDB connection
# --------------------------------------------------
load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DATABASE_NAME = os.getenv("MONGODB_DB", "sentinel")

if not MONGODB_URI:
    raise ValueError("MONGODB_URI is missing from your .env file.")

client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10000)
client.admin.command("ping")

db = client[DATABASE_NAME]
customers = db["customers"]
transactions = db["transactions"]

print("Connected to MongoDB Atlas successfully!")


# --------------------------------------------------
# 2. INSERT_ONE
# Uses the actual CSV field names
# --------------------------------------------------
run_id = str(uuid4())

demo_transaction = {
    "transaction_id": f"DEMO_ONE_{run_id}",
    "customer_id": f"DEMO_CUSTOMER_{run_id}",
    "merchant_id": "DEMO_MERCHANT",
    "device_id": "DEMO_DEVICE",
    "amount_paise": 50000,
    "transaction_amount": 500.00,
    "payment_channel": "demo",
    "merchant_category": "demo",
    "transaction_time": datetime.now(timezone.utc),
    "customer_age": 27,
    "customer_location": {"city": "Pune"},
    "merchant_location": {"city": "Delhi"},
    "account_age": 30,
    "previous_transactions": 10,
    "average_transaction_value": 400.00,
    "failed_transactions": 0,
    "split": "demo",
    "is_synthetic": True,
    "demo_run_id": run_id,
}

transactions.insert_one(demo_transaction)
print("\n1. insert_one: completed")


# --------------------------------------------------
# 3. INSERT_MANY
# --------------------------------------------------
demo_transactions = [
    {
        "transaction_id": f"DEMO_MANY_{i}_{run_id}",
        "customer_id": f"DEMO_CUSTOMER_{run_id}",
        "merchant_id": "DEMO_MERCHANT",
        "amount_paise": amount_paise,
        "transaction_amount": amount_paise / 100,
        "payment_channel": "demo",
        "merchant_category": "demo",
        "transaction_time": datetime.now(timezone.utc),
        "device_id": "DEMO_DEVICE",
        "fraud_demo": False,
        "is_synthetic": True,
        "demo_run_id": run_id,
    }
    for i, amount_paise in enumerate([70000, 90000], start=1)
]

transactions.insert_many(demo_transactions)
print("2. insert_many: completed")


# --------------------------------------------------
# 4. FIND WITH FILTER AND PROJECTION
# --------------------------------------------------
print("\n3. find: transactions for one customer")

cursor = transactions.find(
    {"customer_id": demo_transaction["customer_id"]},
    {
        "_id": 0,
        "transaction_id": 1,
        "customer_id": 1,
        "transaction_amount": 1,
        "merchant_category": 1,
        "payment_channel": 1,
    },
)

for document in cursor:
    print(document)


# --------------------------------------------------
# 5. UPDATE_MANY
# --------------------------------------------------
update_result = transactions.update_many(
    {"demo_run_id": run_id},
    {"$set": {"demo_status": "checked"}},
)

print("\n4. update_many")
print("Matched:", update_result.matched_count)
print("Modified:", update_result.modified_count)


# --------------------------------------------------
# 6. DELETE_ONE
# --------------------------------------------------
delete_result = transactions.delete_one(
    {"transaction_id": demo_transaction["transaction_id"]}
)

print("\n5. delete_one")
print("Deleted:", delete_result.deleted_count)


# Remove the other temporary demo documents.
transactions.delete_many({"demo_run_id": run_id})
print("Temporary demo documents cleaned up.")


# --------------------------------------------------
# 7. CREATE INDEXES
# --------------------------------------------------
transactions.create_index(
    [("customer_id", ASCENDING)],
    name="idx_customer_id",
)

transactions.create_index(
    [("transaction_time", DESCENDING)],
    name="idx_transaction_time",
)

print("\n6. Indexes created:")
print(transactions.index_information())



# --------------------------------------------------
# 8. EXPLAIN BEFORE AND AFTER INDEX
# --------------------------------------------------

# Find a timestamp from the imported dataset
latest_doc = transactions.find_one(
    {"transaction_time": {"$type": "date"}},
    sort=[("transaction_time", DESCENDING)],
    projection={"_id": 0, "transaction_time": 1},
)

if latest_doc:
    latest_time = latest_doc["transaction_time"]

    time_query = {
        "transaction_time": {
            "$gte": latest_time - timedelta(hours=24),
            "$lte": latest_time,
        }
    }

    # BEFORE: drop the time index temporarily if it exists.
    # Only do this in your assignment database, and recreate it below.
    transactions.drop_index("idx_transaction_time")

    before = transactions.find(time_query).explain()

    print("\nEXPLAIN BEFORE INDEX")
    print(before["queryPlanner"]["winningPlan"])

    # AFTER: create the time index and run the same query.
    transactions.create_index(
        [("transaction_time", DESCENDING)],
        name="idx_transaction_time",
    )

    after = transactions.find(time_query).explain()

    print("\nEXPLAIN AFTER INDEX")
    print(after["queryPlanner"]["winningPlan"])

else:
    print("No transaction_time dates found; explain skipped.")

# --------------------------------------------------
# 9. AGGREGATION: TRANSACTION VOLUME BY MERCHANT CATEGORY
# The CSV does not contain fraud_label, so actual
# fraud rates cannot be calculated from this file.
# --------------------------------------------------
category_pipeline = [
    {
        "$group": {
            "_id": "$merchant_category",
            "transaction_count": {"$sum": 1},
            "total_amount": {"$sum": "$transaction_amount"},
            "average_amount": {"$avg": "$transaction_amount"},
        }
    },
    {"$sort": {"transaction_count": -1}},
]

print("\n9. TRANSACTION SUMMARY BY MERCHANT CATEGORY")

for document in transactions.aggregate(category_pipeline):
    print(document)


# --------------------------------------------------
# 10. TOP 10 CUSTOMERS BY AMOUNT IN LAST 24 HOURS
# Uses the actual current time.
# --------------------------------------------------
cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

top_customers_pipeline = [
    {
        "$match": {
            "transaction_time": {"$gte": cutoff}
        }
    },
    {
        "$group": {
            "_id": "$customer_id",
            "transaction_count": {"$sum": 1},
            "total_amount": {"$sum": "$transaction_amount"},
        }
    },
    {"$sort": {"total_amount": -1}},
    {"$limit": 10},
]

print("\n10. TOP 10 CUSTOMERS IN THE LAST 24 HOURS")

top_customers = list(
    transactions.aggregate(top_customers_pipeline)
)

if top_customers:
    for document in top_customers:
        print(document)
else:
    print(
        "No transactions found in the last 24 hours. "
        "The CSV may contain only historical records."
    )


# --------------------------------------------------
# 11. FINAL COUNTS
# --------------------------------------------------
print("\n11. FINAL COLLECTION COUNTS")
print("Customers:", customers.count_documents({}))
print("Transactions:", transactions.count_documents({}))

client.close()
print("\nMongoDB connection closed.")
