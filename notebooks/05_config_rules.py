# Databricks notebook source
spark.sql("""
          CREATE TABLE IF NOT EXISTS workspace.default.pipeline_config (
              rule_name STRING,
              column_name STRING,
              operator STRING,
              value STRING,
              active BOOLEAN,
              description STRING
          )
          USING DELTA
        """)

# COMMAND ----------

spark.sql("""
    INSERT INTO workspace.default.pipeline_config VALUES
        ('filter_qty',      'Quantity',   '>',           '0',    true,  'Eliminar devoluciones y errores'),
        ('filter_price',    'UnitPrice',  '>',           '0',    true,  'Eliminar precios inválidos'),
        ('filter_customer', 'CustomerID', 'is_not_null', null,   true,  'Solo transacciones con cliente'),
        ('calc_revenue',    'total_amount','multiply',   'Quantity*UnitPrice', true, 'Ingreso por linea')
""")

# COMMAND ----------

spark.read.table("workspace.default.pipeline_config").display()