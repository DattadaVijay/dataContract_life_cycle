# Databricks notebook source

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, TimestampType
from datetime import date, datetime

CATALOG = "data_contract_poc"
SCHEMA  = "finance"
TABLE   = "transactions"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"✅ Using {CATALOG}.{SCHEMA}")

# COMMAND ----------

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

dummy_data = [
    Row("TXN001", "ACC001", "Alice Johnson",  date.today(), 1500.00, "USD", "CREDIT", "COMPLETED", "Amazon",     datetime.now()),
    Row("TXN002", "ACC002", "Bob Smith",      date.today(),  200.50, "USD", "DEBIT",  "COMPLETED", "Starbucks",  datetime.now()),
    Row("TXN003", "ACC001", "Alice Johnson",  date.today(), 3200.00, "GBP", "CREDIT", "PENDING",   "Stripe",     datetime.now()),
    Row("TXN004", "ACC003", "Charlie Brown",  date.today(),  450.75, "EUR", "DEBIT",  "COMPLETED", "Netflix",    datetime.now()),
    Row("TXN005", "ACC004", "Diana Prince",   date.today(), 8900.00, "USD", "CREDIT", "COMPLETED", "Salesforce", datetime.now()),
    Row("TXN006", "ACC002", "Bob Smith",      date.today(),  120.00, "USD", "DEBIT",  "FAILED",    "Uber",       datetime.now()),
    Row("TXN007", "ACC005", "Eve Turner",     date.today(), 5600.00, "USD", "CREDIT", "COMPLETED", "Apple",      datetime.now()),
    Row("TXN008", "ACC003", "Charlie Brown",  date.today(),  980.00, "GBP", "DEBIT",  "COMPLETED", "HSBC",       datetime.now()),
]

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

display(spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}"))