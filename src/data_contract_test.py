# Databricks notebook source

# COMMAND ----------

%pip install datacontract-cli[databricks] soda-core soda-core-spark-df --quiet
dbutils.library.restartPython()

# COMMAND ----------

from datacontract.data_contract import DataContract

CONTRACT_PATH = "../contracts/datacontract.yaml"

df = spark.table("data_contract_poc.finance.transactions")
df.createOrReplaceTempView("transactions")

result = DataContract(data_contract_file=CONTRACT_PATH).test()

print(f"Result: {result.result}")
for check in result.checks:
    print(f"  {'✅' if check.result == 'passed' else '❌'} {check.name}: {check.result}")

if result.result == "failed":
    raise Exception("Data contract tests failed.")