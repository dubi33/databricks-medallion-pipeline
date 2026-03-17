# Databricks notebook source
from pyspark.sql.functions import (
    col, sum, countDistinct, to_date
)


# COMMAND ----------

silver_path = "dbfs:/Volumes/workspace/default/silver/online_retail"


# COMMAND ----------

df_silver = spark.read.format("delta").load(silver_path)


# COMMAND ----------

df_clean = (
    df_silver
    .filter(col("Quantity") > 0)
    .filter(col("UnitPrice") > 0)
    .filter(col("CustomerID").isNotNull())
)


# COMMAND ----------

df_clean = df_clean.withColumn(
    "total_amount",
    col("Quantity") * col("UnitPrice")
)


# COMMAND ----------

df_gold_daily = (
    df_clean
    .withColumn("sales_date", to_date(col("InvoiceDate")))
    .groupBy("sales_date")
    .agg(
        sum("total_amount").alias("total_sales"),
        countDistinct("InvoiceNo").alias("num_invoices")
    )
)


# COMMAND ----------

df_gold_daily.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/Volumes/workspace/default/gold/sales_daily")


# COMMAND ----------

df_gold_country = (
    df_clean
    .groupBy("Country")
    .agg(
        sum("total_amount").alias("total_sales"),
        countDistinct("InvoiceNo").alias("num_invoices")
    )
)


# COMMAND ----------

df_gold_country.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/Volumes/workspace/default/gold/sales_country")


# COMMAND ----------

df_gold_products = (
    df_clean
    .groupBy("StockCode", "Description")
    .agg(
        sum("Quantity").alias("total_units_sold"),
        sum("total_amount").alias("total_sales")
    )
    .orderBy(col("total_sales").desc())
)


# COMMAND ----------

df_gold_products.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/Volumes/workspace/default/gold/top_products")


# COMMAND ----------

df_gold_daily.display()
df_gold_country.display()
df_gold_products.limit(10).display()
