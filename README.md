#  Databricks Medallion Architecture — Online Retail Pipeline

Pipeline de datos end-to-end implementado en **Databricks** usando la **arquitectura medallón** (Raw → Bronze → Silver → Gold) con **Delta Lake**, **Unity Catalog** y **reglas de negocio dinámicas**.

---

##  Arquitectura

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
  ┌──────────────────────────────────┐
  │  pipeline_config (Delta Table)   │  ← Reglas de negocio dinámicas
  └──────────────┬───────────────────┘
                 │ .collect() en runtime
                 ▼
  ┌─────────────┐
  │    GOLD     │  Agregaciones listas para análisis de negocio
  └─────────────┘
         ▲
  ┌─────────────┐
  │   RUNNER    │  Orquestador + Databricks Jobs (schedule diario)
  └─────────────┘
```

---

##  Estructura del Repositorio

```
databricks-medallion-pipeline/
│
├── notebooks/
│   ├── 00_setup_environment.sql     # Creación de volúmenes en Unity Catalog
│   ├── 01_ec2_to_raw.py             # Validación y carga del archivo fuente
│   ├── 02_raw_to_bronze.py          # Ingesta a capa Bronze con metadatos
│   ├── 03_bronze_to_silver.py       # Limpieza y transformación de datos
│   ├── 04_silver_to_gold.py         # Lee reglas dinámicas → tablas Gold
│   ├── 05_config_rules.py           # Crea y llena tabla de reglas de negocio
│   └── 06_pipeline_runner.py        # Orquestador maestro del pipeline
│
├── assets/
│   └── medallion_architecture.png
│
└── README.md
```

---

##  Descripción de cada notebook

### `00_setup_environment` — Configuración del entorno
Crea los **4 volúmenes** en Unity Catalog que actúan como las capas del pipeline:

```sql
CREATE VOLUME IF NOT EXISTS workspace.default.raw;
CREATE VOLUME IF NOT EXISTS workspace.default.bronze;
CREATE VOLUME IF NOT EXISTS workspace.default.silver;
CREATE VOLUME IF NOT EXISTS workspace.default.gold;
```

---

### `01_ec2_to_raw` — EC2 → RAW
Valida la llegada del archivo fuente al volumen `raw/ec2_source/`. El CSV de **Online Retail** se deposita sin ninguna transformación, preservando los datos originales.

---

### `02_raw_to_bronze` — RAW → BRONZE
Lee el CSV desde `raw`, añade **metadatos de trazabilidad** y persiste en **Delta Lake**:

- `ingestion_timestamp` → marca temporal de ingesta
- `source_file` → ruta del archivo origen (via `_metadata.file_path`)

>  Permite auditoría completa: saber cuándo y desde dónde llegó cada registro.

---

### `03_bronze_to_silver` — BRONZE → SILVER
Normaliza el campo `InvoiceDate` que venía en formato inconsistente (`M/D/YYYY H:MM`):

1. Separación de fecha y hora con `split()`
2. Padding de dígitos con `lpad()`
3. Reconstrucción con `concat_ws()`
4. Conversión a `TimestampType` con `to_timestamp()`

>  Datos confiables, tipados y listos para análisis.

---

### `05_config_rules` — Tabla de reglas de negocio 
Crea la tabla `pipeline_config` en Unity Catalog y la llena con las reglas base. **Se ejecuta una sola vez**.

```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.default.pipeline_config (
        rule_name   STRING,
        column_name STRING,
        operator    STRING,
        value       STRING,
        active      BOOLEAN,
        description STRING
    ) USING DELTA
""")

spark.sql("""
    INSERT INTO workspace.default.pipeline_config VALUES
        ('filter_qty',      'Quantity',     '>',         '0',                 true, 'Eliminar devoluciones y errores'),
        ('filter_price',    'UnitPrice',    '>',         '0',                 true, 'Eliminar precios inválidos'),
        ('filter_customer', 'CustomerID',   'is_not_null', null,              true, 'Solo transacciones con cliente'),
        ('calc_revenue',    'total_amount', 'multiply',  'Quantity*UnitPrice', true, 'Ingreso por línea')
""")
```

#### Estructura de la tabla

| Columna | Tipo | Descripción |
|---|---|---|
| `rule_name` | STRING | Identificador único de la regla |
| `column_name` | STRING | Columna del DataFrame sobre la que aplica |
| `operator` | STRING | Operación: `>`, `<`, `>=`, `is_not_null`, `multiply` |
| `value` | STRING | Valor de comparación o cálculo |
| `active` | BOOLEAN | `true` activa la regla, `false` la desactiva |
| `description` | STRING | Explicación legible de la regla |

---

### `04_silver_to_gold` — SILVER → GOLD (dinámico) 
En lugar de filtros hardcodeados, el notebook **lee las reglas activas** de `pipeline_config` y las aplica en runtime:

```python
# 1. Leer solo las reglas activas
rules = spark.read.table("workspace.default.pipeline_config") \
    .filter(col("active") == True) \
    .collect()

df_clean = df_silver

# 2. Aplicar filtros dinámicamente según el operador
for rule in rules:
    if rule["operator"] == ">":
        df_clean = df_clean.filter(col(rule["column_name"]) > float(rule["value"]))
        print(f" {rule['rule_name']} → {rule['description']}")

    elif rule["operator"] == "is_not_null":
        df_clean = df_clean.filter(col(rule["column_name"]).isNotNull())
        print(f" {rule['rule_name']} → {rule['description']}")

# 3. Aplicar cálculos dinámicamente
for rule in rules:
    if rule["operator"] == "multiply":
        left, right = rule["value"].split("*")
        df_clean = df_clean.withColumn(
            rule["column_name"],
            col(left.strip()) * col(right.strip())
        )
        print(f" {rule['rule_name']} → {rule['description']}")
```

#### ¿Cómo detecta reglas nuevas o desactivadas?

```
pipeline_config (Delta Table)
    │
    │  .filter(active == True).collect()  ← solo trae las activas
    ▼
04_silver_to_gold
    │
    ├── operador ">"           → aplica filter()
    ├── operador "is_not_null" → aplica filter()
    └── operador "multiply"    → aplica withColumn()
```

Si se agrega una regla nueva o se desactiva una existente, el pipeline lo detecta automáticamente en la próxima ejecución **sin modificar ningún notebook**.

#### Tablas Gold generadas

| Tabla Delta | Descripción |
|---|---|
| `gold/sales_daily` | Ventas totales y número de facturas por día |
| `gold/sales_country` | Ventas totales y facturas por país |
| `gold/top_products` | Productos más vendidos (unidades + ingresos) |

---

### `06_pipeline_runner` — Orquestador maestro 
Ejecuta todo el pipeline en orden desde un solo punto de entrada:

```python
run_notebook("02_raw_to_bronze",    step=1)
run_notebook("03_bronze_to_silver", step=2)
run_notebook("04_silver_to_gold",   step=3)
```

Programado con **Databricks Jobs** para ejecutarse automáticamente todos los días.

>  En producción nadie corre notebooks a mano. El Job se encarga de todo.

---

##  Gestión de reglas de negocio

### Ver el estado actual de todas las reglas

```python
spark.read.table("workspace.default.pipeline_config").display()
```

---

### Agregar una nueva regla

```python
# Filtrar transacciones con UnitPrice mayor a $1
spark.sql("""
    INSERT INTO workspace.default.pipeline_config VALUES
        ('filter_min_price', 'UnitPrice', '>', '1', true, 'Excluir micro transacciones')
""")

# Filtrar solo transacciones del Reino Unido
spark.sql("""
    INSERT INTO workspace.default.pipeline_config VALUES
        ('filter_country', 'Country', '=', 'United Kingdom', true, 'Solo transacciones UK')
""")

# Filtrar cantidades menores a 5 unidades
spark.sql("""
    INSERT INTO workspace.default.pipeline_config VALUES
        ('filter_min_qty', 'Quantity', '>=', '5', true, 'Mínimo 5 unidades por línea')
""")
```

En la próxima ejecución del pipeline, el `04_silver_to_gold` detecta las reglas nuevas y las aplica automáticamente.

---

### Desactivar una regla (sin eliminarla)

```python
# Desactivar temporalmente el filtro de CustomerID
spark.sql("""
    UPDATE workspace.default.pipeline_config
    SET active = false
    WHERE rule_name = 'filter_customer'
""")
```

> La regla queda registrada en la tabla pero el pipeline la ignora porque `active = false`. Delta Lake guarda el historial del cambio.

---

### Reactivar una regla

```python
spark.sql("""
    UPDATE workspace.default.pipeline_config
    SET active = true
    WHERE rule_name = 'filter_customer'
""")
```

---

### Modificar el valor de una regla

```python
# Cambiar el filtro de Quantity de > 0 a > 2
spark.sql("""
    UPDATE workspace.default.pipeline_config
    SET value = '2',
        description = 'Excluir líneas de 1 o 2 unidades'
    WHERE rule_name = 'filter_qty'
""")
```

---

### Ver el historial de cambios (Delta Lake Time Travel)

```python
# Ver todos los cambios registrados en la tabla
spark.sql("""
    DESCRIBE HISTORY workspace.default.pipeline_config
""").display()

# Ver cómo estaba la tabla en la versión 0 (estado inicial)
spark.read \
    .option("versionAsOf", 0) \
    .table("workspace.default.pipeline_config") \
    .display()
```

---

##  Automatización con Databricks Jobs

```
Databricks Jobs
  └── medallion-pipeline-daily
        ├── Schedule: todos los días 08:00 AM (America/Bogota)
        ├── Notebook: 06_pipeline_runner
        ├── Cluster: existing cluster
        └── Notifications: email en fallo o éxito
```

Flujo automático diario:
```
02:30 AM → Job trigger
              └── 06_pipeline_runner
                      ├──  02_raw_to_bronze      (~45s)
                      ├──  03_bronze_to_silver   (~30s)
                      └──  04_silver_to_gold     (~28s)
                               └── lee pipeline_config
                                   y aplica reglas activas
```

---

##  Stack Tecnológico

| Herramienta | Uso |
|---|---|
| **Databricks** | Plataforma de procesamiento y orquestación |
| **Apache Spark (PySpark)** | Transformaciones distribuidas |
| **Delta Lake** | Formato de almacenamiento transaccional |
| **Unity Catalog** | Gobernanza y gestión de datos |
| **Databricks Jobs** | Scheduling y automatización del pipeline |
| **DBFS Volumes** | Almacenamiento de archivos por capa |

---

##  Dataset

**Online Retail Dataset** — Transacciones de e-commerce de una tienda del Reino Unido (2010–2011).
Campos: `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`.

---

##  ¿Cómo ejecutar?

1. Subir los notebooks al Workspace de Databricks
2. Ejecutar `00_setup_environment` → crea los volúmenes
3. Ejecutar `01_ec2_to_raw` → valida el archivo fuente
4. Ejecutar `05_config_rules` → crea la tabla de reglas (**solo una vez**)
5. Configurar el Job en Databricks Workflows apuntando a `06_pipeline_runner`
6. Clic en **Run now** para validar, luego el schedule se encarga de todo

---

## 👤 Autor

**[Duban Daniel Granados Mendez]** 
