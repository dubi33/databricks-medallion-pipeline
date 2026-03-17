# Databricks notebook source
from pyspark.sql.functions import (
    col, trim, length,
    try_to_timestamp, coalesce
)


# COMMAND ----------

bronze_path = "dbfs:/Volumes/workspace/default/bronze/online_retail/"
silver_path = "dbfs:/Volumes/workspace/default/silver/online_retail/"


# COMMAND ----------

df_bronze = spark.read.format("delta").load(bronze_path)


# COMMAND ----------

total_rows = df_bronze.count()
print(f"Filas en Bronze: {total_rows}")


# COMMAND ----------

df_bronze.printSchema()


# COMMAND ----------

for c in df_bronze.columns:
    nulls = df_bronze.filter(col(c).isNull()).count()
    print(f"{c}: {nulls} NULLs")


# COMMAND ----------

df_bronze.filter(col("Quantity") <= 0).count()


# COMMAND ----------

from pyspark.sql.functions import split

df_step1 = df_bronze.withColumn(
    "date_part",
    split(col("InvoiceDate"), " ").getItem(0)
).withColumn(
    "time_part",
    split(col("InvoiceDate"), " ").getItem(1)
)


# COMMAND ----------

from pyspark.sql.functions import split

df_step2 = (
    df_step1
    .withColumn("month",   split(col("date_part"), "/").getItem(0))
    .withColumn("day", split(col("date_part"), "/").getItem(1))
    .withColumn("year",  split(col("date_part"), "/").getItem(2))
)


# COMMAND ----------

df_step3 = (
    df_step2
    .withColumn("hour",   split(col("time_part"), ":").getItem(0))
    .withColumn("minute", split(col("time_part"), ":").getItem(1))
)


# COMMAND ----------

from pyspark.sql.functions import lpad

df_step4 = (
    df_step3
    .withColumn("day",    lpad(col("day"), 2, "0"))
    .withColumn("month",  lpad(col("month"), 2, "0"))
    .withColumn("hour",   lpad(col("hour"), 2, "0"))
    .withColumn("minute", lpad(col("minute"), 2, "0"))
)


# COMMAND ----------

from pyspark.sql.functions import concat_ws

df_step5 = df_step4.withColumn(
    "InvoiceDate_clean",
    concat_ws(
        " ",
        concat_ws("/", col("day"), col("month"), col("year")),
        concat_ws(":", col("hour"), col("minute"))
    )
)


# COMMAND ----------

from pyspark.sql.functions import to_timestamp

df_silver = df_step5.withColumn(
    "InvoiceDate",
    to_timestamp(col("InvoiceDate_clean"), "dd/MM/yyyy HH:mm")
)


# COMMAND ----------

df_silver = df_silver.drop(
    "date_part", "time_part",
    "day", "month", "year",
    "hour", "minute",
    "InvoiceDate_clean"
)


# COMMAND ----------

df_silver.printSchema()
df_silver.filter(col("InvoiceDate").isNull()).count()


# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .save(silver_path)
)
