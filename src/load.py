"""Phases 6 and 7: Gold tables and SQLite loading."""

from __future__ import annotations

import decimal
import json
import math
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
)

try:
    from .utils import (
        create_process_id,
        create_spark_session,
        ensure_directories,
        find_latest_parquet_folder,
        list_real_parquet_files,
        load_config,
        path_from_config,
        spark_path,
    )
except ImportError:  # pragma: no cover
    from utils import (  # type: ignore
        create_process_id,
        create_spark_session,
        ensure_directories,
        find_latest_parquet_folder,
        list_real_parquet_files,
        load_config,
        path_from_config,
        spark_path,
    )


REQUIRED_GOLD_SOURCE_COLUMNS = [
    "trip_id",
    "service_type",
    "pickup_datetime",
    "dropoff_datetime",
    "trip_duration_minutes",
    "trip_distance",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "tip_percentage",
    "average_speed_mph",
    "year",
    "month",
    "source_file",
    "is_suspicious_trip",
]


TABLES_TO_LOAD = {
    "gold_trips_clean": "gold_path",
    "gold_daily_revenue": "gold_path",
    "gold_location_performance": "gold_path",
    "quality_rejected_records": "quarantine_path",
    "quality_metrics_summary": "audit_path",
    "audit_file_inventory": "audit_path",
}


SQL_VALIDATION_QUERIES = {
    "revenue_by_service": """
SELECT
    service_type,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM gold_trips_clean
GROUP BY service_type
ORDER BY total_revenue DESC;
""",
    "quality_metrics": """
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
""",
    "top_revenue_routes": """
SELECT
    pickup_location_id,
    dropoff_location_id,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration
FROM gold_trips_clean
GROUP BY pickup_location_id, dropoff_location_id
ORDER BY total_revenue DESC
LIMIT 20;
""",
}


def load_latest_validated_silver(spark: SparkSession, silver_path: str | Path) -> DataFrame:
    """Read the latest validated Silver dataset for Gold generation."""
    latest = find_latest_parquet_folder(
        silver_path,
        name_prefixes=["validated_trips_", "trips_validated", "silver_trips_valid"],
    )
    if latest is None:
        raise FileNotFoundError("No validated Silver dataset was found. Run Phase 5 first.")
    return spark.read.parquet(spark_path(latest))


def load_latest_quality_metrics(spark: SparkSession, audit_path: str | Path) -> Optional[DataFrame]:
    """Read the latest quality_metrics_summary, if present."""
    latest = find_latest_parquet_folder(audit_path, name_contains="quality_metrics_summary")
    if latest is None:
        return None
    return spark.read.parquet(spark_path(latest))


def prepare_gold_source(silver_validated_trips: DataFrame) -> DataFrame:
    """Ensure required Gold source columns exist with controlled NULL values."""
    result = silver_validated_trips
    for column_name in REQUIRED_GOLD_SOURCE_COLUMNS:
        if column_name in result.columns:
            continue
        if column_name in [
            "trip_duration_minutes",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "total_amount",
            "tip_percentage",
            "average_speed_mph",
        ]:
            result = result.withColumn(column_name, F.lit(None).cast(DoubleType()))
        elif column_name in ["pickup_location_id", "dropoff_location_id", "payment_type", "year", "month"]:
            result = result.withColumn(column_name, F.lit(None).cast(IntegerType()))
        elif column_name == "is_suspicious_trip":
            result = result.withColumn(column_name, F.lit(False))
        elif column_name in ["pickup_datetime", "dropoff_datetime"]:
            result = result.withColumn(column_name, F.lit(None).cast(TimestampType()))
        else:
            result = result.withColumn(column_name, F.lit(None).cast(StringType()))
    return result


def build_gold_trips_clean(gold_source: DataFrame) -> DataFrame:
    """Build granular clean trips table."""
    return gold_source.select(
        F.col("trip_id").cast(StringType()).alias("trip_id"),
        F.col("service_type").cast(StringType()).alias("service_type"),
        F.col("pickup_datetime").cast(TimestampType()).alias("pickup_datetime"),
        F.col("dropoff_datetime").cast(TimestampType()).alias("dropoff_datetime"),
        F.round(F.col("trip_duration_minutes").cast(DoubleType()), 2).alias("trip_duration_minutes"),
        F.round(F.col("trip_distance").cast(DoubleType()), 2).alias("trip_distance"),
        F.col("pickup_location_id").cast(IntegerType()).alias("pickup_location_id"),
        F.col("dropoff_location_id").cast(IntegerType()).alias("dropoff_location_id"),
        F.col("payment_type").cast(IntegerType()).alias("payment_type"),
        F.round(F.col("fare_amount").cast(DoubleType()), 2).alias("fare_amount"),
        F.round(F.col("tip_amount").cast(DoubleType()), 2).alias("tip_amount"),
        F.round(F.col("total_amount").cast(DoubleType()), 2).alias("total_amount"),
        F.round(F.col("tip_percentage").cast(DoubleType()), 2).alias("tip_percentage"),
        F.round(F.col("average_speed_mph").cast(DoubleType()), 2).alias("average_speed_mph"),
        F.col("year").cast(IntegerType()).alias("year"),
        F.col("month").cast(IntegerType()).alias("month"),
        F.col("source_file").cast(StringType()).alias("source_file"),
    )


def build_gold_daily_revenue(
    gold_source: DataFrame,
    quality_metrics_summary: Optional[DataFrame],
) -> DataFrame:
    """Build daily revenue aggregate enriched with quality metrics."""
    daily_base = (
        gold_source.withColumn("trip_date", F.to_date("pickup_datetime"))
        .groupBy("service_type", "year", "month", "trip_date")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("average_fare"),
            F.round(F.avg("tip_amount"), 2).alias("average_tip"),
            F.round(F.avg("trip_distance"), 2).alias("average_trip_distance"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("average_trip_duration"),
        )
    )

    if quality_metrics_summary is None:
        return daily_base.select(
            "service_type",
            "trip_date",
            "total_trips",
            "total_revenue",
            "average_fare",
            "average_tip",
            "average_trip_distance",
            "average_trip_duration",
            F.lit(0).cast(IntegerType()).alias("rejected_records"),
            F.lit(100.0).cast(DoubleType()).alias("quality_percentage"),
        )

    metrics = (
        quality_metrics_summary.select(
            F.col("service_type").cast(StringType()).alias("qm_service_type"),
            F.col("year").cast(IntegerType()).alias("qm_year"),
            F.col("month").cast(IntegerType()).alias("qm_month"),
            F.col("rejected_records").cast(IntegerType()).alias("rejected_records"),
            F.col("quality_percentage").cast(DoubleType()).alias("quality_percentage"),
        )
        .dropDuplicates(["qm_service_type", "qm_year", "qm_month"])
    )

    return daily_base.join(
        metrics,
        (daily_base["service_type"] == metrics["qm_service_type"])
        & (daily_base["year"] == metrics["qm_year"])
        & (daily_base["month"] == metrics["qm_month"]),
        "left",
    ).select(
        daily_base["service_type"],
        daily_base["trip_date"],
        daily_base["total_trips"],
        daily_base["total_revenue"],
        daily_base["average_fare"],
        daily_base["average_tip"],
        daily_base["average_trip_distance"],
        daily_base["average_trip_duration"],
        F.coalesce(F.col("rejected_records"), F.lit(0)).cast(IntegerType()).alias("rejected_records"),
        F.coalesce(F.col("quality_percentage"), F.lit(100.0)).cast(DoubleType()).alias("quality_percentage"),
    )


def build_gold_location_performance(gold_source: DataFrame) -> DataFrame:
    """Build route performance aggregate."""
    return gold_source.groupBy("service_type", "pickup_location_id", "dropoff_location_id").agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2).alias("average_fare"),
        F.round(F.avg("trip_distance"), 2).alias("average_distance"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("average_duration"),
        F.sum(F.when(F.col("is_suspicious_trip") == True, F.lit(1)).otherwise(F.lit(0)))
        .cast(IntegerType())
        .alias("suspicious_trip_count"),
    )


def build_gold_tables(
    silver_validated_trips: DataFrame,
    quality_metrics_summary: Optional[DataFrame],
) -> Dict[str, DataFrame]:
    """Build all required Gold tables."""
    gold_source = prepare_gold_source(silver_validated_trips)
    return {
        "gold_trips_clean": build_gold_trips_clean(gold_source),
        "gold_daily_revenue": build_gold_daily_revenue(gold_source, quality_metrics_summary),
        "gold_location_performance": build_gold_location_performance(gold_source),
    }


def write_gold_outputs(
    spark: SparkSession,
    gold_tables: Dict[str, DataFrame],
    gold_path: str | Path,
    audit_path: str | Path,
    process_id: str,
) -> Dict[str, Path]:
    """Write required Gold tables and an audit summary."""
    gold_path = Path(gold_path)
    audit_path = Path(audit_path)
    output_paths = {
        "gold_trips_clean": gold_path / f"gold_trips_clean_{process_id}",
        "gold_daily_revenue": gold_path / f"gold_daily_revenue_{process_id}",
        "gold_location_performance": gold_path / f"gold_location_performance_{process_id}",
    }

    (
        gold_tables["gold_trips_clean"].repartition("service_type", "year", "month")
        .write.mode("overwrite")
        .partitionBy("service_type", "year", "month")
        .parquet(spark_path(output_paths["gold_trips_clean"]))
    )
    (
        gold_tables["gold_daily_revenue"].repartition("service_type")
        .write.mode("overwrite")
        .partitionBy("service_type")
        .parquet(spark_path(output_paths["gold_daily_revenue"]))
    )
    (
        gold_tables["gold_location_performance"].repartition("service_type")
        .write.mode("overwrite")
        .partitionBy("service_type")
        .parquet(spark_path(output_paths["gold_location_performance"]))
    )

    audit_rows = [
        {
            "process_id": process_id,
            "table_name": table_name,
            "output_path": str(output_path),
            "record_count": gold_tables[table_name].count(),
            "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        for table_name, output_path in output_paths.items()
    ]
    audit_gold_path = audit_path / f"audit_gold_generation_{process_id}"
    spark.createDataFrame(audit_rows).write.mode("overwrite").parquet(spark_path(audit_gold_path))
    output_paths["audit_gold_generation"] = audit_gold_path
    return output_paths


def quote_identifier(name: str) -> str:
    """Escape table or column names for SQLite."""
    return '"' + str(name).replace('"', '""') + '"'


def spark_type_to_sqlite(data_type: Any) -> str:
    """Map Spark data types to SQLite affinities."""
    if isinstance(data_type, (IntegerType, LongType, ShortType, ByteType)):
        return "INTEGER"
    if isinstance(data_type, (DoubleType, FloatType, DecimalType)):
        return "REAL"
    if isinstance(data_type, BooleanType):
        return "INTEGER"
    if isinstance(data_type, (TimestampType, DateType)):
        return "TEXT"
    return "TEXT"


def normalize_value(value: Any) -> Any:
    """Convert Spark/Python values to SQLite-safe values."""
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value, ensure_ascii=False)
    return value


def create_sqlite_table(conn: sqlite3.Connection, table_name: str, df: DataFrame) -> None:
    """Create a SQLite table from a Spark DataFrame schema."""
    columns_sql = [
        f"{quote_identifier(field.name)} {spark_type_to_sqlite(field.dataType)}"
        for field in df.schema.fields
    ]
    create_sql = f"CREATE TABLE {quote_identifier(table_name)} (\n    " + ",\n    ".join(columns_sql) + "\n);"
    conn.execute(f"DROP TABLE IF EXISTS {quote_identifier(table_name)};")
    conn.execute(create_sql)


def load_spark_df_to_sqlite(
    conn: sqlite3.Connection,
    table_name: str,
    df: DataFrame,
    batch_size: int = 5000,
) -> int:
    """Load a Spark DataFrame into SQLite by batches, without Pandas as the engine."""
    create_sqlite_table(conn, table_name, df)

    columns = df.columns
    quoted_columns = ", ".join(quote_identifier(column) for column in columns)
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql = f"INSERT INTO {quote_identifier(table_name)} ({quoted_columns}) VALUES ({placeholders});"

    batch = []
    inserted_rows = 0
    for row in df.toLocalIterator():
        row_dict = row.asDict(recursive=False)
        batch.append(tuple(normalize_value(row_dict.get(column)) for column in columns))

        if len(batch) >= batch_size:
            conn.executemany(insert_sql, batch)
            inserted_rows += len(batch)
            batch = []

    if batch:
        conn.executemany(insert_sql, batch)
        inserted_rows += len(batch)

    conn.commit()
    return inserted_rows


def discover_pipeline_tables(config: Dict[str, Any]) -> Dict[str, Path]:
    """Discover the latest Parquet folder for each mandatory database table."""
    discovered: Dict[str, Path] = {}
    missing = []

    for table_name, path_key in TABLES_TO_LOAD.items():
        base_path = path_from_config(config, path_key)
        latest = find_latest_parquet_folder(base_path, name_contains=table_name)
        if latest is None or not list_real_parquet_files(latest):
            missing.append(table_name)
        else:
            discovered[table_name] = latest

    if missing:
        raise FileNotFoundError(
            "Missing mandatory tables for database load: " + ", ".join(missing)
        )
    return discovered


def read_discovered_tables(spark: SparkSession, discovered_tables: Dict[str, Path]) -> Dict[str, DataFrame]:
    """Read discovered Parquet tables with Spark."""
    return {
        table_name: spark.read.parquet(spark_path(table_path))
        for table_name, table_path in discovered_tables.items()
    }


def configure_sqlite_connection(database_path: str | Path) -> sqlite3.Connection:
    """Open SQLite and apply local write pragmas."""
    database_path = Path(database_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(database_path)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    conn.execute("PRAGMA cache_size = -200000;")
    return conn


def load_tables_to_sqlite(
    spark_tables: Dict[str, DataFrame],
    database_path: str | Path,
    process_id: str,
    batch_size: int = 5000,
) -> List[Dict[str, Any]]:
    """Load mandatory pipeline tables to SQLite in a stable order."""
    load_order = [
        "audit_file_inventory",
        "quality_metrics_summary",
        "quality_rejected_records",
        "gold_daily_revenue",
        "gold_location_performance",
        "gold_trips_clean",
    ]
    conn = configure_sqlite_connection(database_path)
    load_results: List[Dict[str, Any]] = []

    try:
        for table_name in load_order:
            inserted_rows = load_spark_df_to_sqlite(
                conn=conn,
                table_name=table_name,
                df=spark_tables[table_name],
                batch_size=batch_size,
            )
            load_results.append(
                {
                    "process_id": process_id,
                    "table_name": table_name,
                    "inserted_rows": inserted_rows,
                    "loaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

        write_database_load_audit(conn, load_results)
        return load_results
    finally:
        conn.close()


def write_database_load_audit(conn: sqlite3.Connection, load_results: List[Dict[str, Any]]) -> None:
    """Create audit_database_load in SQLite."""
    conn.execute("DROP TABLE IF EXISTS audit_database_load;")
    conn.execute(
        """
CREATE TABLE audit_database_load (
    process_id TEXT,
    table_name TEXT,
    inserted_rows INTEGER,
    loaded_at TEXT
);
"""
    )
    conn.executemany(
        "INSERT INTO audit_database_load (process_id, table_name, inserted_rows, loaded_at) VALUES (?, ?, ?, ?);",
        [
            (
                item["process_id"],
                item["table_name"],
                item["inserted_rows"],
                item["loaded_at"],
            )
            for item in load_results
        ],
    )
    conn.commit()


def run_sql_validation_queries(database_path: str | Path) -> Dict[str, List[tuple]]:
    """Run the three mandatory SQL validation queries."""
    conn = sqlite3.connect(database_path)
    try:
        return {
            name: list(conn.execute(query))
            for name, query in SQL_VALIDATION_QUERIES.items()
        }
    finally:
        conn.close()


def run_gold_generation(
    spark: SparkSession | None = None,
    config_path: str | Path | None = None,
    process_id: str | None = None,
    write_outputs: bool = True,
) -> Dict[str, Any]:
    """Run Phase 6 Gold generation."""
    config = load_config(config_path)
    spark = spark or create_spark_session("fase06_gold", config)
    process_id = process_id or create_process_id("fase6")

    silver_path = path_from_config(config, "silver_path")
    audit_path = path_from_config(config, "audit_path")
    gold_path = path_from_config(config, "gold_path")
    ensure_directories([gold_path, audit_path])

    silver_validated = load_latest_validated_silver(spark, silver_path)
    quality_metrics = load_latest_quality_metrics(spark, audit_path)
    gold_tables = build_gold_tables(silver_validated, quality_metrics)

    result: Dict[str, Any] = {
        "process_id": process_id,
        "gold_tables": gold_tables,
    }

    if write_outputs:
        result["output_paths"] = write_gold_outputs(spark, gold_tables, gold_path, audit_path, process_id)

    return result


def run_database_load(
    spark: SparkSession | None = None,
    config_path: str | Path | None = None,
    process_id: str | None = None,
    batch_size: int = 5000,
    run_validation_queries: bool = True,
) -> Dict[str, Any]:
    """Run Phase 7 database load into SQLite."""
    config = load_config(config_path)
    spark = spark or create_spark_session("fase07_carga_sqlite", config)
    process_id = process_id or create_process_id("fase7")

    database_path = path_from_config(config, "database_path")
    discovered = discover_pipeline_tables(config)
    spark_tables = read_discovered_tables(spark, discovered)
    load_results = load_tables_to_sqlite(spark_tables, database_path, process_id, batch_size=batch_size)

    result: Dict[str, Any] = {
        "process_id": process_id,
        "database_path": database_path,
        "discovered_tables": discovered,
        "load_results": load_results,
    }
    if run_validation_queries:
        result["validation_queries"] = run_sql_validation_queries(database_path)
    return result


if __name__ == "__main__":
    gold_result = run_gold_generation()
    print("Phase 6 completed:", gold_result["process_id"])
    load_result = run_database_load()
    print("Phase 7 completed:", load_result["process_id"])
