# Databricks Medallion Architecture — Online Retail Pipeline

Pipeline de datos end-to-end implementado en **Databricks** usando la **arquitectura medallón** (Raw → Bronze → Silver → Gold) con **Delta Lake** y **Unity Catalog**.

---

## Arquitectura

```
EC2 / Fuente Externa
        │
        ▼
  ┌─────────────┐
  │     RAW     │  Archivos CSV originales sin modificar
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │   BRONZE    │  Ingesta cruda + metadatos de trazabilidad
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │   SILVER    │  Datos limpios, tipados y validados
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │    GOLD     │  Agregaciones listas para análisis de negocio
  └─────────────┘
```

---

## Estructura del Repositorio

```
databricks-medallion-pipeline/
│
├── notebooks/
│   ├── 00_setup_environment.sql     # Creación de volúmenes en Unity Catalog
│   ├── 01_ec2_to_raw.py             # Validación y carga del archivo fuente
│   ├── 02_raw_to_bronze.py          # Ingesta a capa Bronze con metadatos
│   ├── 03_bronze_to_silver.py       # Limpieza y transformación de datos
│   └── 04_silver_to_gold.py         # Agregaciones analíticas (Gold)
│
├── assets/
│   └── architecture_diagram.png
│
└── README.md
```

---

## Descripción de cada capa

### `00_setup_environment` — Configuración del entorno
Crea los **4 volúmenes** en Unity Catalog (`workspace.default`) que actúan como las capas del pipeline:

```sql
CREATE VOLUME IF NOT EXISTS workspace.default.raw;
CREATE VOLUME IF NOT EXISTS workspace.default.bronze;
CREATE VOLUME IF NOT EXISTS workspace.default.silver;
CREATE VOLUME IF NOT EXISTS workspace.default.gold;
```

---

### `01_ec2_to_raw` — EC2 → RAW
Valida la llegada del archivo fuente al volumen `raw/ec2_source/`.  
El archivo CSV de **Online Retail** se deposita en esta capa sin ninguna transformación, preservando los datos originales.

```python
dbutils.fs.mkdirs("dbfs:/Volumes/workspace/default/raw/ec2_source")
dbutils.fs.ls("dbfs:/Volumes/workspace/default/raw/ec2_source")
```

---

### `02_raw_to_bronze` — RAW → BRONZE
Lee el CSV desde `raw`, añade **metadatos de trazabilidad** y persiste en formato **Delta Lake**:

- `ingestion_timestamp` → marca temporal de ingesta
- `source_file` → ruta del archivo origen (via `_metadata.file_path`)

```python
df_bronze = (
    df_raw
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
)
df_bronze.write.format("delta").mode("append").save(bronze_path)
```

>  Permite auditoría completa: saber cuándo y desde dónde llegó cada registro.

---

### `03_bronze_to_silver` — BRONZE → SILVER
La capa de **calidad de datos**. Aplica múltiples transformaciones sobre el campo `InvoiceDate` que venía en formato inconsistente (`M/D/YYYY H:MM`):

1. Separación de fecha y hora
2. Padding de dígitos (`lpad`) para normalizar el formato
3. Reconstrucción como `dd/MM/yyyy HH:mm`
4. Conversión a `TimestampType` con `to_timestamp`
5. Validación de nulos post-transformación

```python
df_silver = df_step5.withColumn(
    "InvoiceDate",
    to_timestamp(col("InvoiceDate_clean"), "dd/MM/yyyy HH:mm")
)
```

>  Datos confiables, tipados y listos para análisis.

---

### `04_silver_to_gold` — SILVER → GOLD
Genera **3 tablas analíticas** a partir de los datos limpios, filtrando registros inválidos (`Quantity > 0`, `UnitPrice > 0`, `CustomerID NOT NULL`):

| Tabla Delta | Descripción |
|---|---|
| `gold/sales_daily` | Ventas totales y número de facturas por día |
| `gold/sales_country` | Ventas totales y facturas por país |
| `gold/top_products` | Productos más vendidos (unidades + ingresos) |

```python
df_gold_daily = (
    df_clean
    .withColumn("sales_date", to_date(col("InvoiceDate")))
    .groupBy("sales_date")
    .agg(
        sum("total_amount").alias("total_sales"),
        countDistinct("InvoiceNo").alias("num_invoices")
    )
)
```

>  Listas para conectar con herramientas de BI como Power BI o Tableau.

---

##  Stack Tecnológico

| Herramienta | Uso |
|---|---|
| **Databricks** | Plataforma de procesamiento y orquestación |
| **Apache Spark (PySpark)** | Transformaciones distribuidas |
| **Delta Lake** | Formato de almacenamiento transaccional |
| **Unity Catalog** | Gobernanza y gestión de datos |
| **DBFS Volumes** | Almacenamiento de archivos por capa |
| **SQL** | Creación de volúmenes y validaciones |

---

##  Dataset

**Online Retail Dataset** — Transacciones de e-commerce de una tienda del Reino Unido (2010–2011).  
Campos principales: `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`.

---

##  ¿Cómo ejecutar?

1. Clonar el repositorio y subir los notebooks a tu Workspace de Databricks
2. Ejecutar en orden: `00` → `01` → `02` → `03` → `04`
3. Verificar los volúmenes creados en el **Catalog** de Unity Catalog
4. Explorar las tablas Gold con SQL o conectar a un dashboard de BI

---

##  Autor

**[Duban Daniel Granados Mendez]**  
[LinkedIn](www.linkedin.com/in/duban-daniel-granados-mendez-317077283) · 
