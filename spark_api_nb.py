# Databricks notebook source
import pyspark.sql.functions as F

# Set up pathing
source_path = '/tmp/dlt/source_data'

bz_write_path = '/tmp/dlt/population_data_bz'
bz_checkpoint_path = '/tmp/dlt/population_data_bz/_checkpoints'

# COMMAND ----------

def extract_data():
  return (spark.readStream.format('cloudFiles')
    .option('cloudFiles.format', 'csv')
    .option('header', 'true')
    .schema('city string, year int, population long')
    .load(source_path))

# COMMAND ----------

(extract_data().writeStream.format('delta')
  .option('checkpointLocation', bz_checkpoint_path)
  .trigger(once=True)
  .start(bz_write_path))

# COMMAND ----------

sv_write_path = '/tmp/dlt/population_data_sv'
sv_checkpoint_path = '/tmp/dlt/population_data_sv/sv_checkpoints'

# COMMAND ----------

def transfrom_data():
  return (spark.read.format('delta')
    .load(bz_write_path)
    .groupby('year')
    .agg(F.sum('population').alias("total_population")))

# COMMAND ----------

transfrom_data().write.format('delta').mode("overwrite").save(sv_write_path)
