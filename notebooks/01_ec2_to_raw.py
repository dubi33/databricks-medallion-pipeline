# Databricks notebook source
# crear carpeta
dbutils.fs.mkdirs("dbfs:/Volumes/workspace/default/raw/ec2_source")

# COMMAND ----------

# ya se subio manualmente a dbfs:/Volumes/workspace/default/raw/ec2_source y ahora se valida la existencia del archivo en dbfs
dbutils.fs.ls("dbfs:/Volumes/workspace/default/raw/ec2_source")