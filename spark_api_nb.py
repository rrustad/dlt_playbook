# Databricks notebook source
import pyspark.sql.functions as F
import dlt

# COMMAND ----------

@dlt.table()
def population_data():
  return (spark.readStream.format('cloudFiles')
  .option('cloudFiles.format', 'csv')
  .option('header', 'true')
  .schema('city string, year int, population long')
  .load('/tmp/dlt/source_data'))

# COMMAND ----------

# (population_data().writeStream.format('delta')
#   .option('checkpointLocation', '/tmp/dlt/population_data_bz/_checkpoints')
#   .trigger(once=True)
#   .table("riley_test.population_data"))

# COMMAND ----------

@dlt.table()
def population_agg():
  return(spark.table("LIVE.population_data")
  .groupby('year')
  .agg(F.sum('population').alias("total_population")))


# COMMAND ----------

# population_agg().write.format('delta').mode("overwrite").saveAsTable('population_agg')
