# Proyecto II Parcial - Modelado Avanzado de Bases de Datos

## Descripción

Este proyecto implementa un pipeline ETL utilizando PySpark para el procesamiento de archivos Parquet de viajes de taxi de NYC TLC.

El pipeline fue desarrollado siguiendo una arquitectura Lakehouse con capas Raw, Bronze, Silver, Gold, Audit y Quarantine.

Las principales actividades realizadas fueron:

* Extracción de datos.
* Auditoría de archivos.
* Diagnóstico y reconstrucción de esquemas.
* Recuperación de archivos dañados.
* Transformación de datos.
* Validación de calidad.
* Enriquecimiento analítico.
* Carga en base de datos SQLite.
* Validación mediante consultas SQL.

---

# Estructura del proyecto

```text
etl_spark_parquet_advanced/
│
├── config/
│   └── etl_config.yaml
│
├── data/
│   ├── audit/
│   ├── bronze/
│   ├── gold/
│   ├── quarantine/
│   ├── raw/
│   └── silver/
│
├── metadata/
│   ├── canonical_schema_trips.json
│   ├── expected_schema_fhvhv.json
│   ├── expected_schema_green.json
│   ├── expected_schema_yellow.json
│   ├── homologation_matrix.json
│   └── business_rules.json
│
├── notebooks/
│   ├── 01_extraccion.ipynb
│   ├── 02_diagnostico_reconstruccion.ipynb
│   ├── 03_transformacion_validacion.ipynb
│   ├── 04_carga_base_datos.ipynb
│   └── 05_reporte_calidad_conclusiones.ipynb
│
├── src/
│   ├── __init__.py
│   ├── extract.py
│   ├── schema_recovery.py
│   ├── transformations.py
│   ├── quality_rules.py
│   ├── load.py
│   └── utils.py
│
└── README.md
```

---

# Cómo descargar los datos

Se utilizaron archivos Parquet del dataset NYC Taxi Trip Records.

Los archivos deben colocarse dentro de:

```text
data/raw/
```

Distribución utilizada:

```text
raw/
├── yellow/
├── green/
├── fhvhv/
└── bad_parquet/
```

La carpeta `bad_parquet` contiene archivos utilizados para las pruebas de recuperación y cuarentena.

---
# Cómo preparar el entorno

## Requisitos del sistema

El proyecto fue desarrollado en:

* Windows 10 / Windows 11
* Python 3.11
* Java JDK 17 
* Apache Spark 3.x
* Jupyter Notebook

---

## Paso 1: Verificar Python

Abrir CMD o PowerShell y ejecutar:

```bash
python --version
```

o

```bash
py --version
```

Debe mostrarse una versión de Python instalada correctamente.

---

## Paso 2: Verificar Java

Spark requiere Java para funcionar.

Ejecutar:

```bash
java -version
```

Si Java está instalado correctamente, se mostrará la versión del JDK.

---

## Paso 3: Instalar librerías necesarias

Instalar las dependencias utilizadas en el proyecto:

```bash
pip install pyspark
pip install pandas
pip install pyarrow
pip install notebook
pip install PyYAML
```

También se puede instalar todo junto desde el archivo de dependencias del proyecto:

```bash
python -m pip install -r requirements.txt
```

Verificar que PySpark funcione:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

---

## Paso 4: Configurar Hadoop para Windows

Para permitir que Spark escriba archivos correctamente en Windows, se utilizó Hadoop local.

Crear la carpeta:

```text
C:\hadoop\bin
```

Copiar dentro:

```text
winutils.exe
hadoop.dll
```

Configurar la variable de entorno:

```text
HADOOP_HOME=C:\hadoop
```

Agregar también:

```text
C:\hadoop\bin
```

al PATH del sistema.

Reiniciar la terminal después de realizar los cambios.

---

## Paso 5: Iniciar Jupyter Notebook

Abrir CMD o PowerShell en la carpeta del proyecto y ejecutar:

```bash
jupyter notebook
```

Si el comando no funciona:

```bash
py -m notebook
```

---

## Paso 6: Configuración utilizada por PySpark

En los notebooks se configuró:

```python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
```

Esto garantiza que Spark utilice la misma instalación de Python utilizada por Jupyter.

---

## Paso 7: Verificar la estructura del proyecto

Antes de ejecutar los notebooks, verificar que existan las carpetas:

```text
data/raw
data/bronze
data/silver
data/gold
data/quarantine
data/audit
metadata
config
notebooks
```

y que los archivos Parquet se encuentren dentro de:

```text
data/raw/
```

---

## Paso 8: Ejecutar los notebooks

Los notebooks deben ejecutarse en el siguiente orden:

```text
01_extraccion.ipynb
02_diagnostico_reconstruccion.ipynb
03_transformacion_validacion.ipynb
04_carga_base_datos.ipynb
05_reporte_calidad_conclusiones.ipynb
```

No se debe ejecutar un notebook sin haber finalizado correctamente el anterior.


# Configuración utilizada

Para la ejecución del proyecto se configuró PySpark en Windows utilizando:

```python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
```

También se utilizó Hadoop local para permitir la ejecución de Spark en Windows.

---

# Cómo ejecutar el pipeline

Los notebooks deben ejecutarse en el siguiente orden.

También se dejó una implementación modular en `src/`, con la misma lógica del flujo desarrollado en notebooks. Esta versión permite reutilizar el pipeline por fases:

```bash
python -m src.extract
python -m src.schema_recovery
python -m src.transformations
python -m src.quality_rules
python -m src.load
```

La ejecución modular conserva salidas idempotentes mediante `mode("overwrite")` y genera un `process_id` por fase.

## Notebook 1

```text
01_extraccion.ipynb
```

Funciones:

* Lectura de archivos Parquet.
* Auditoría inicial.
* Inventario técnico de archivos.

Genera:

```text
audit_file_inventory
```

---

## Notebook 2

```text
02_diagnostico_reconstruccion.ipynb
```

Funciones:

* Diagnóstico de esquemas.
* Reconstrucción canónica.
* Recuperación de archivos dañados.
* Gestión de cuarentena.

Genera:

```text
data/bronze/
data/quarantine/
```

---

## Notebook 3

```text
03_transformacion_validacion.ipynb
```

Funciones:

* Transformación de datos.
* Validación de calidad.
* Generación de registros rechazados.
* Generación de métricas de calidad.

Genera:

```text
data/silver/
quality_rejected_records
quality_metrics_summary
```

---

## Notebook 4

```text
04_carga_base_datos.ipynb
```

Funciones:

* Generación de tablas Gold.
* Carga a SQLite.
* Ejecución de consultas SQL.

Genera:

```text
gold_trips_clean
gold_daily_revenue
gold_location_performance
```

---

## Notebook 5

```text
05_reporte_calidad_conclusiones.ipynb
```

Funciones:

* Presentación de resultados.
* Métricas finales.
* Conclusiones del proyecto.

---

# Parámetros configurables

Los principales parámetros utilizados durante el proyecto son:

```python
BASE_PATH
RAW_PATH
BRONZE_PATH
SILVER_PATH
GOLD_PATH
AUDIT_PATH
QUARANTINE_PATH
DATABASE_PATH
PROCESS_ID
```

El parámetro `PROCESS_ID` identifica cada ejecución del pipeline y permite mantener trazabilidad.

---

# Base de datos utilizada

Se utilizó SQLite.

Archivo generado:

```text
data/database/nyc_taxi_lakehouse.db
```

Tablas cargadas:

* gold_trips_clean
* gold_daily_revenue
* gold_location_performance
* quality_rejected_records
* quality_metrics_summary
* audit_file_inventory

---

# Cómo validar los resultados

## Consulta 1: Ingresos por servicio

```sql
SELECT
    service_type,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue
FROM gold_trips_clean
GROUP BY service_type
ORDER BY total_revenue DESC;
```

## Consulta 2: Métricas de calidad

```sql
SELECT
    service_type,
    year,
    month,
    total_records,
    valid_records,
    rejected_records,
    quality_percentage
FROM quality_metrics_summary
ORDER BY year, month, service_type;
```

## Consulta 3: Rutas con mayor recaudación

```sql
SELECT
    pickup_location_id,
    dropoff_location_id,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(trip_duration_minutes) AS avg_duration
FROM gold_trips_clean
GROUP BY pickup_location_id, dropoff_location_id
ORDER BY total_revenue DESC
LIMIT 20;
```

---

# Técnicas de optimización implementadas

Durante el desarrollo del proyecto se implementaron las siguientes técnicas:

1. Uso de esquemas explícitos mediante StructType.
2. Lectura selectiva de columnas.
3. Escritura particionada por servicio, año y mes.
4. Partition pruning mediante particiones físicas.
5. Manejo del problema de archivos pequeños utilizando coalesce().
6. Control del número de archivos de salida.
7. Arquitectura Lakehouse con capas Raw, Bronze, Silver, Gold, Audit y Quarantine.

---

# Resultado final

El pipeline permite procesar archivos Parquet reales, detectar errores, recuperar esquemas dañados, validar calidad de datos, generar tablas analíticas y cargar la información final en una base de datos SQLite para su posterior consulta mediante SQL.
