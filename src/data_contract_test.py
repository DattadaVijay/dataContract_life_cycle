# Databricks notebook source

# COMMAND ----------

%pip install datacontract-cli[databricks] soda-core soda-core-spark-df --quiet
dbutils.library.restartPython()

# COMMAND ----------

import os
from datacontract.data_contract import DataContract

# Read from base_parameters passed by the job
dbutils.widgets.text("DATACONTRACT_DATABRICKS_TOKEN", "")
dbutils.widgets.text("DATACONTRACT_DATABRICKS_HOST", "")
dbutils.widgets.text("DATACONTRACT_DATABRICKS_HTTP_PATH", "")

os.environ["DATACONTRACT_DATABRICKS_TOKEN"]     = dbutils.widgets.get("DATACONTRACT_DATABRICKS_TOKEN")
os.environ["DATACONTRACT_DATABRICKS_HOST"]      = dbutils.widgets.get("DATACONTRACT_DATABRICKS_HOST")
os.environ["DATACONTRACT_DATABRICKS_HTTP_PATH"] = dbutils.widgets.get("DATACONTRACT_DATABRICKS_HTTP_PATH")

# COMMAND ----------

CONTRACT_PATH = "/Volumes/governance_cat/contract_mgmt/contract"

df = spark.table("data_contract_poc.finance.transactions")
df.createOrReplaceTempView("transactions")

result = DataContract(data_contract_file=CONTRACT_PATH).test()

print(f"Result: {result.result}")
for check in result.checks:
    print(f"  {'✅' if check.result == 'passed' else '❌'} {check.name}: {check.result}")

if result.result == "failed":
    raise Exception("Data contract tests failed.")