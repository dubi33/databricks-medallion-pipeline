# Databricks notebook source
from pyspark.sql.functions import (
    col, sum, countDistinct, to_date
)


# COMMAND ----------

silver_path = "dbfs:/Volumes/workspace/default/silver/online_retail"


# COMMAND ----------

df_silver = spark.read.format("delta").load(silver_path)


# COMMAND ----------

# DBTITLE 1,Cell 4: Fix NameError col import
from pyspark.sql.functions import col
# Leer reglas activas desde la tabla de configuración
rules = spark.read.table("workspace.default.pipeline_config") \
    .filter(col("active") == True) \
    .collect()

# COMMAND ----------

df_clean = df_silver

for rule in rules:
    if rule["operator"] == ">":
        df_clean = df_clean.filter(col(rule["column_name"]) > float(rule["value"]))
        print(f" Filtro aplicado: {rule['rule_name']} → {rule['description']}")

    elif rule["operator"] == "is_not_null":
        df_clean = df_clean.filter(col(rule["column_name"]).isNotNull())
        print(f" Filtro aplicado: {rule['rule_name']} → {rule['description']}")

print(f"\n Filas después de filtros: {df_clean.count()}")

# COMMAND ----------

for rule in rules:
    if rule["operator"] == "multiply":
        left, right = rule["value"].split("*")
        df_clean = df_clean.withColumn(
            rule["column_name"],
            col(left.strip()) * col(right.strip())
        )
        print(f" Cálculo aplicado: {rule['rule_name']} → {rule['description']}")

# COMMAND ----------

# sales_daily
df_gold_daily = (
    df_clean
    .withColumn("sales_date", to_date(col("InvoiceDate")))
    .groupBy("sales_date")
    .agg(
        sum("total_amount").alias("total_sales"),
        countDistinct("InvoiceNo").alias("num_invoices")
    )
)
df_gold_daily.write.format("delta").mode("overwrite") \
    .save("dbfs:/Volumes/workspace/default/gold/sales_daily")

# sales_country
df_gold_country = (
    df_clean
    .groupBy("Country")
    .agg(
        sum("total_amount").alias("total_sales"),
        countDistinct("InvoiceNo").alias("num_invoices")
    )
)
df_gold_country.write.format("delta").mode("overwrite") \
    .save("dbfs:/Volumes/workspace/default/gold/sales_country")

# top_products
df_gold_products = (
    df_clean
    .groupBy("StockCode", "Description")
    .agg(
        sum("Quantity").alias("total_units_sold"),
        sum("total_amount").alias("total_sales")
    )
    .orderBy(col("total_sales").desc())
)
df_gold_products.write.format("delta").mode("overwrite") \
    .save("dbfs:/Volumes/workspace/default/gold/top_products")

print(" 3 tablas Gold generadas correctamente")

# COMMAND ----------

print("── sales_daily ──")
df_gold_daily.limit(5).display()

print("── sales_country ──")
df_gold_country.limit(5).display()

print("── top_products ──")
df_gold_products.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC De aqui en adelante es la version anterior fija

# COMMAND ----------

# df_gold_daily = (
#     df_clean
#     .withColumn("sales_date", to_date(col("InvoiceDate")))
#    .groupBy("sales_date")
#     .agg(
#         sum("total_amount").alias("total_sales"),
#         countDistinct("InvoiceNo").alias("num_invoices")
#     )
# )


# COMMAND ----------

# df_clean = df_clean.withColumn(
#     "total_amount",
#     col("Quantity") * col("UnitPrice")
# )


# COMMAND ----------

# df_gold_daily.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .save("dbfs:/Volumes/workspace/default/gold/sales_daily")


# COMMAND ----------

# df_gold_country = (
#     df_clean
#     .groupBy("Country")
#     .agg(
#         sum("total_amount").alias("total_sales"),
#         countDistinct("InvoiceNo").alias("num_invoices")
#     )
# )


# COMMAND ----------

# df_gold_country.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .save("dbfs:/Volumes/workspace/default/gold/sales_country")


# COMMAND ----------

# df_gold_products = (
#     df_clean
#     .groupBy("StockCode", "Description")
#     .agg(
#         sum("Quantity").alias("total_units_sold"),
#         sum("total_amount").alias("total_sales")
#     )
#     .orderBy(col("total_sales").desc())
# )


# COMMAND ----------

# df_clean = (
#     df_silver
#     .filter(col("Quantity") > 0)
#     .filter(col("UnitPrice") > 0)
#     .filter(col("CustomerID").isNotNull())
# )


# COMMAND ----------

# df_gold_products.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .save("dbfs:/Volumes/workspace/default/gold/top_products")


# COMMAND ----------

# df_gold_daily.display()
# df_gold_country.display()
# df_gold_products.limit(10).display()
