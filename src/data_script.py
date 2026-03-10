# Databricks notebook source

# COMMAND ----------

CATALOG = "data_contract_poc"
SCHEMA  = "finance"
TABLE   = "transactions"

# COMMAND ----------
# Create catalog and schema

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"✅ Using {CATALOG}.{SCHEMA}")

# COMMAND ----------
# Create table

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.{TABLE} (
        transaction_id     STRING,
        account_id         STRING,
        customer_name      STRING,
        transaction_date   DATE,
        amount             DOUBLE,
        currency           STRING,
        transaction_type   STRING,
        status             STRING,
        merchant           STRING,
        created_at         TIMESTAMP
    )
    USING DELTA
""")

print(f"✅ Table {CATALOG}.{SCHEMA}.{TABLE} ready")

# COMMAND ----------
# Insert dummy data

from pyspark.sql import Row
from datetime import date, datetime

dummy_data = [
    Row("TXN001", "ACC001", "Alice Johnson",  date(2024, 1, 5),  1500.00, "USD", "CREDIT", "COMPLETED", "Amazon",       datetime.now()),
    Row("TXN002", "ACC002", "Bob Smith",      date(2024, 1, 6),   200.50, "USD", "DEBIT",  "COMPLETED", "Starbucks",    datetime.now()),
    Row("TXN003", "ACC001", "Alice Johnson",  date(2024, 1, 7),  3200.00, "GBP", "CREDIT", "PENDING",   "Stripe",       datetime.now()),
    Row("TXN004", "ACC003", "Charlie Brown",  date(2024, 1, 8),   450.75, "EUR", "DEBIT",  "COMPLETED", "Netflix",      datetime.now()),
    Row("TXN005", "ACC004", "Diana Prince",   date(2024, 1, 9),  8900.00, "USD", "CREDIT", "COMPLETED", "Salesforce",   datetime.now()),
    Row("TXN006", "ACC002", "Bob Smith",      date(2024, 1, 10),  120.00, "USD", "DEBIT",  "FAILED",    "Uber",         datetime.now()),
    Row("TXN007", "ACC005", "Eve Turner",     date(2024, 1, 11), 5600.00, "USD", "CREDIT", "COMPLETED", "Apple",        datetime.now()),
    Row("TXN008", "ACC003", "Charlie Brown",  date(2024, 1, 12),  980.00, "GBP", "DEBIT",  "COMPLETED", "HSBC",         datetime.now()),
]

from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, TimestampType

schema = StructType([
    StructField("transaction_id",   StringType()),
    StructField("account_id",       StringType()),
    StructField("customer_name",    StringType()),
    StructField("transaction_date", DateType()),
    StructField("amount",           DoubleType()),
    StructField("currency",         StringType()),
    StructField("transaction_type", StringType()),
    StructField("status",           StringType()),
    StructField("merchant",         StringType()),
    StructField("created_at",       TimestampType()),
])

df = spark.createDataFrame(dummy_data, schema)

df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE}")

print(f"✅ {df.count()} rows written")

# COMMAND ----------
# Verify

display(spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}"))
