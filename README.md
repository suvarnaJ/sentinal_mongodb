# Project SENTINEL — MongoDB with Python

## 1. Objective

Model a subset of the Project SENTINEL payment transaction data using MongoDB Atlas and Python's PyMongo library. Demonstrate document modelling, CRUD operations, indexing, query-plan analysis, and aggregation pipelines.

## 2. Business Problem

PayVista needs to analyse payment transactions to identify fraud patterns, understand transaction behaviour across merchant categories, and support better fraud detection decisions.

MongoDB stores the selected transaction data as flexible BSON documents and supports filtering, indexing, and aggregation.

## 3. Collections

### customers
Stores customer profile information, including `customer_id`, `customer_age`, `account_age`, customer location, and associated device IDs.

### transactions
Stores individual transaction records, including transaction ID, customer ID, amount, merchant category, transaction time, device ID, location fields, historical activity features, and fraud label.

## 4. Embedding vs Referencing

Customer profiles and transactions are stored separately. Transactions reference customers using `customer_id`.

This avoids duplicating customer profile information in every transaction and supports analysis across large transaction histories. Device IDs are stored in an array within each customer document for this project, provided the source data supports that relationship.

## 5. Technology Stack

- MongoDB Atlas
- Python
- PyMongo
- pandas
- python-dotenv

## 6. Setup

1. Create a MongoDB Atlas cluster and database user.
2. Configure Network Access with the required IP address.
3. Add the Atlas URI and database name to `.env`.
4. Install dependencies using `pip install -r requirements.txt`.
5. Place the authorised SENTINEL CSV export in `data/sentinel.csv`.
6. Run `python ingest.py`.
7. Run `python main.py`.

Never commit `.env` or confidential source data to GitHub.

## 7. Implemented Operations

- `insert_one`
- `insert_many`
- `find` with filters and projection
- `update_many`
- `delete_one`
- Indexes on `customer_id` and `transaction_time`
- `explain()` before and after indexing
- Fraud rate by merchant category
- Top 10 customers by transaction amount over the last 24 hours

## 8. Validation

Record the actual number of customer and transaction documents after ingestion. Verify that at least 5,000 transaction records were loaded.

Capture the query plans before and after indexing and explain whether the query uses a collection scan or an index scan.

## 9. Data Limitations

Use the actual source schema and document any missing values, duplicate IDs, or differences between the CSV and the field dictionary. Fraud-rate calculations depend on reliable, finalised fraud labels.

## 10. Conclusion

This project demonstrates how MongoDB can represent payment data as documents and how Python can load, query, update, and analyse that data using PyMongo.
