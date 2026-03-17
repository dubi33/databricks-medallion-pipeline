# Databricks notebook source

# Creacion de paths tipo variables de entorno
source_path = "dbfs:/Volumes/workspace/default/raw/ec2_source/"
bronze_path = "dbfs:/Volumes/workspace/default/bronze/online_retail/"


# COMMAND ----------

df_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(source_path)
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

df_bronze = (
    df_raw
    .withColumn("ingestion_timestap", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
)

# COMMAND ----------

df_bronze.write.format("delta").mode("append").save(bronze_path)

# COMMAND ----------

# validar 
spark.read.format("delta").load(bronze_path).display()