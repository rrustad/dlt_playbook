# Databricks notebook source
# MAGIC %sh
# MAGIC mkdir /dbfs/tmp/dlt/source_data

# COMMAND ----------

# MAGIC %sh
# MAGIC echo """
# MAGIC city,year,population
# MAGIC Seattle metro,2019,3406000
# MAGIC Seattle metro,2020,3433000
# MAGIC """ > /dbfs/tmp/dlt/source_data/data.csv
# MAGIC 
# MAGIC echo """
# MAGIC city,year,population
# MAGIC Portland metro,2019,2127000
# MAGIC Portland metro,2020,2151000
# MAGIC """ > /dbfs/tmp/dlt/source_data/data2.csv
