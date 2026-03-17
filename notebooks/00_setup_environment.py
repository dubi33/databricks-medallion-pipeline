# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.raw;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.bronze;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.silver;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.default;