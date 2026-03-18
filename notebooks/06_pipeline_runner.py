# Databricks notebook source
# Obtener la ruta dinámica desde el notebook actual
NOTEBOOK_BASE = "/".join(
    dbutils.notebook.entry_point
    .getDbutils().notebook()
    .getContext().notebookPath().get()
    .split("/")[:-1]
)

print(f"Ruta base detectada: {NOTEBOOK_BASE}")
TIMEOUT = 0

# COMMAND ----------

import time

def run_notebook(name, step):
    print(f"{'─'*50}")
    print(f" Paso {step}: {name}")
    start = time.time()
    
    try:
        dbutils.notebook.run(f"{NOTEBOOK_BASE}/{name}", TIMEOUT)
        elapsed = round(time.time() - start, 1)
        print(f" Completado en {elapsed}s")
    except Exception as e:
        print(f" Error en {name}: {str(e)}")
        raise

# COMMAND ----------

print(" Iniciando pipeline...")
print(f"{'═'*50}")

start_total = time.time()

run_notebook("02_raw_to_bronze",   step=1)
run_notebook("03_bronze_to_silver", step=2)
run_notebook("04_silver_to_gold",  step=3)

total = round(time.time() - start_total, 1)

print(f"{'═'*50}")
print(f" Pipeline finalizado en {total}s")
print(f" Tablas Gold generadas: sales_daily · sales_country · top_products")

# COMMAND ----------

print(" Validando tablas Gold...\n")

tables = [
    ("sales_daily",    "dbfs:/Volumes/workspace/default/gold/sales_daily"),
    ("sales_country",  "dbfs:/Volumes/workspace/default/gold/sales_country"),
    ("top_products",   "dbfs:/Volumes/workspace/default/gold/top_products"),
]

for name, path in tables:
    count = spark.read.format("delta").load(path).count()
    print(f" {name}: {count} filas")
