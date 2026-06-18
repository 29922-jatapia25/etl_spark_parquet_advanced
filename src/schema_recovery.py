"""Phases 2 and 3: schema diagnosis, canonical recovery and quarantine."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

try:
    from .utils import (
        clean_error_message,
        create_process_id,
        create_spark_session,
        ensure_directories,
        extract_partition_value,
        find_latest_parquet_folder,
        load_config,
        load_json,
        path_from_config,
        sanitize_path_fragment,
        schema_hash,
        spark_path,
        union_all_by_name,
    )
except ImportError:  # pragma: no cover
    from utils import (  # type: ignore
        clean_error_message,
        create_process_id,
        create_spark_session,
        ensure_directories,
        extract_partition_value,
        find_latest_parquet_folder,
        load_config,
        load_json,
        path_from_config,
        sanitize_path_fragment,
        schema_hash,
        spark_path,
        union_all_by_name,
    )


RECUPERABLE_SCHEMA_MISMATCH = "RECUPERABLE_SCHEMA_MISMATCH"
RECUPERABLE_MISSING_COLUMNS = "RECUPERABLE_MISSING_COLUMNS"
RECUPERABLE_TYPE_CASTING = "RECUPERABLE_TYPE_CASTING"
PARTIALLY_RECOVERABLE = "PARTIALLY_RECOVERABLE"
NOT_RECOVERABLE_CORRUPT_METADATA = "NOT_RECOVERABLE_CORRUPT_METADATA"
NOT_RECOVERABLE_EMPTY_FILE = "NOT_RECOVERABLE_EMPTY_FILE"
NOT_RECOVERABLE_UNSUPPORTED_FORMAT = "NOT_RECOVERABLE_UNSUPPORTED_FORMAT"

RECOVERABLE = "RECOVERABLE"
PARTIALLY_RECOVERABLE_STATUS = "PARTIALLY_RECOVERABLE"
NOT_RECOVERABLE = "NOT_RECOVERABLE"


DEFAULT_CANONICAL_SCHEMA = {
    "trip_id": "string",
    "service_type": "string",
    "vendor_id": "long",
    "pickup_datetime": "timestamp",
    "dropoff_datetime": "timestamp",
    "passenger_count": "double",
    "trip_distance": "double",
    "pickup_location_id": "long",
    "dropoff_location_id": "long",
    "payment_type": "long",
    "fare_amount": "double",
    "extra_amount": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
    "year": "integer",
    "month": "integer",
    "source_file": "string",
    "ingestion_timestamp": "timestamp",
    "quality_status": "string",
}


DEFAULT_EXPECTED_SCHEMAS = {
    "yellow": {
        "VendorID": "numeric",
        "tpep_pickup_datetime": "timestamp",
        "tpep_dropoff_datetime": "timestamp",
        "passenger_count": "numeric",
        "trip_distance": "numeric",
        "PULocationID": "numeric",
        "DOLocationID": "numeric",
        "payment_type": "numeric",
        "fare_amount": "numeric",
        "extra": "numeric",
        "mta_tax": "numeric",
        "tip_amount": "numeric",
        "tolls_amount": "numeric",
        "total_amount": "numeric",
        "congestion_surcharge": "numeric",
        "airport_fee": "numeric",
    },
    "green": {
        "VendorID": "numeric",
        "lpep_pickup_datetime": "timestamp",
        "lpep_dropoff_datetime": "timestamp",
        "passenger_count": "numeric",
        "trip_distance": "numeric",
        "PULocationID": "numeric",
        "DOLocationID": "numeric",
        "payment_type": "numeric",
        "fare_amount": "numeric",
        "extra": "numeric",
        "mta_tax": "numeric",
        "tip_amount": "numeric",
        "tolls_amount": "numeric",
        "total_amount": "numeric",
        "congestion_surcharge": "numeric",
    },
    "fhvhv": {
        "pickup_datetime": "timestamp",
        "dropoff_datetime": "timestamp",
        "PULocationID": "numeric",
        "DOLocationID": "numeric",
        "trip_miles": "numeric",
        "base_passenger_fare": "numeric",
        "tolls": "numeric",
        "sales_tax": "numeric",
        "tips": "numeric",
    },
}


COLUMN_EQUIVALENCES = {
    "vendor_id": ["VendorID", "vendor_id"],
    "pickup_datetime": ["pickup_datetime", "tpep_pickup_datetime", "lpep_pickup_datetime"],
    "dropoff_datetime": ["dropoff_datetime", "tpep_dropoff_datetime", "lpep_dropoff_datetime"],
    "passenger_count": ["passenger_count"],
    "trip_distance": ["trip_distance", "trip_miles"],
    "pickup_location_id": ["pickup_location_id", "PULocationID"],
    "dropoff_location_id": ["dropoff_location_id", "DOLocationID"],
    "payment_type": ["payment_type"],
    "fare_amount": ["fare_amount", "base_passenger_fare"],
    "extra_amount": ["extra_amount", "extra"],
    "mta_tax": ["mta_tax", "sales_tax"],
    "tip_amount": ["tip_amount", "tips"],
    "tolls_amount": ["tolls_amount", "tolls"],
    "total_amount": ["total_amount"],
    "congestion_surcharge": ["congestion_surcharge"],
    "airport_fee": ["airport_fee"],
    "year": ["year"],
    "month": ["month"],
    "source_file": ["source_file"],
    "quality_status": ["quality_status"],
}


SCHEMA_DIAGNOSIS_SCHEMA = StructType(
    [
        StructField("process_id", StringType(), True),
        StructField("service_type", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("schema_hash", StringType(), True),
        StructField("actual_column_count", IntegerType(), True),
        StructField("expected_column_count", IntegerType(), True),
        StructField("missing_columns", StringType(), True),
        StructField("additional_columns", StringType(), True),
        StructField("incompatible_types", StringType(), True),
        StructField("reconstruction_status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("diagnosed_at", StringType(), True),
    ]
)


PHASE2_QUARANTINE_SCHEMA = StructType(
    [
        StructField("process_id", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("service_type", StringType(), True),
        StructField("stage", StringType(), True),
        StructField("classification", StringType(), True),
        StructField("technical_reason", StringType(), True),
        StructField("recommended_action", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)


PROBLEM_QUARANTINE_SCHEMA = StructType(
    [
        StructField("process_id", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("file_size_mb", DoubleType(), True),
        StructField("problem_category", StringType(), True),
        StructField("recoverability", StringType(), True),
        StructField("error_type", StringType(), True),
        StructField("read_status", StringType(), True),
        StructField("reconstruction_status", StringType(), True),
        StructField("record_count", IntegerType(), True),
        StructField("column_count", IntegerType(), True),
        StructField("missing_columns", StringType(), True),
        StructField("additional_columns", StringType(), True),
        StructField("incompatible_types", StringType(), True),
        StructField("exception_message", StringType(), True),
        StructField("failed_stage", StringType(), True),
        StructField("rejection_reason", StringType(), True),
        StructField("recommended_action", StringType(), True),
        StructField("processed_at", StringType(), True),
    ]
)


def load_expected_schemas(metadata_path: str | Path) -> Dict[str, Dict[str, str]]:
    """Load expected source schemas from metadata, falling back to defaults."""
    metadata = Path(metadata_path)
    expected = {}
    for service in ["yellow", "green", "fhvhv"]:
        schema_file = metadata / f"expected_schema_{service}.json"
        expected[service] = load_json(schema_file) if schema_file.exists() else DEFAULT_EXPECTED_SCHEMAS[service]
    return expected


def load_canonical_schema(metadata_path: str | Path) -> Dict[str, str]:
    """Load the canonical trip schema."""
    schema_file = Path(metadata_path) / "canonical_schema_trips.json"
    if schema_file.exists():
        return load_json(schema_file)
    return DEFAULT_CANONICAL_SCHEMA.copy()


def normalize_type(data_type: Any) -> str:
    """Group Spark data types into coarse categories for schema comparison."""
    dtype = data_type.simpleString().lower()

    if "timestamp" in dtype or "date" in dtype:
        return "timestamp"
    if any(token in dtype for token in ["int", "bigint", "long", "double", "float", "decimal"]):
        return "numeric"
    if "string" in dtype:
        return "string"
    if "boolean" in dtype:
        return "boolean"

    return dtype


def read_file_schema(spark: SparkSession, file_path: str | Path) -> Tuple[DataFrame, Dict[str, str], str]:
    """Read a Parquet file and return the DataFrame plus schema metadata."""
    df = spark.read.parquet(spark_path(file_path))
    actual_schema = {field.name: normalize_type(field.dataType) for field in df.schema.fields}
    return df, actual_schema, schema_hash(df.schema, mode="simple")


def _row_value(row: Row | Dict[str, Any], key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    return row[key]


def diagnose_schemas(
    spark: SparkSession,
    readable_rows: Iterable[Row],
    expected_schemas: Dict[str, Dict[str, str]],
    process_id: str,
) -> DataFrame:
    """Compare real schemas against the expected service schemas."""
    rows: List[Dict[str, Any]] = []

    for row in readable_rows:
        service_type = str(_row_value(row, "service_type"))
        file_name = str(_row_value(row, "file_name"))
        file_path = Path(str(_row_value(row, "file_path")))
        expected_schema = expected_schemas.get(service_type, {})

        try:
            _, actual_schema, file_schema_hash = read_file_schema(spark, file_path)
            actual_columns = set(actual_schema.keys())
            expected_columns = set(expected_schema.keys())

            missing_columns = sorted(expected_columns - actual_columns)
            additional_columns = sorted(actual_columns - expected_columns)
            incompatible_types = [
                f"{col}: esperado={expected_type}, real={actual_schema[col]}"
                for col, expected_type in expected_schema.items()
                if col in actual_schema and actual_schema[col] != expected_type
            ]

            if not missing_columns and not incompatible_types:
                reconstruction_status = "RECUPERABLE_OK"
            elif missing_columns and not incompatible_types:
                reconstruction_status = RECUPERABLE_MISSING_COLUMNS
            elif incompatible_types:
                reconstruction_status = RECUPERABLE_TYPE_CASTING
            else:
                reconstruction_status = PARTIALLY_RECOVERABLE

            rows.append(
                {
                    "process_id": process_id,
                    "service_type": service_type,
                    "file_name": file_name,
                    "file_path": str(file_path),
                    "schema_hash": file_schema_hash,
                    "actual_column_count": len(actual_columns),
                    "expected_column_count": len(expected_columns),
                    "missing_columns": ", ".join(missing_columns),
                    "additional_columns": ", ".join(additional_columns),
                    "incompatible_types": ", ".join(incompatible_types),
                    "reconstruction_status": reconstruction_status,
                    "error_message": "",
                    "diagnosed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
        except Exception as exc:  # noqa: BLE001
            rows.append(
                {
                    "process_id": process_id,
                    "service_type": service_type,
                    "file_name": file_name,
                    "file_path": str(file_path),
                    "schema_hash": "",
                    "actual_column_count": 0,
                    "expected_column_count": len(expected_schema),
                    "missing_columns": "",
                    "additional_columns": "",
                    "incompatible_types": "",
                    "reconstruction_status": NOT_RECOVERABLE_CORRUPT_METADATA,
                    "error_message": clean_error_message(exc),
                    "diagnosed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

    return spark.createDataFrame(rows, schema=SCHEMA_DIAGNOSIS_SCHEMA)


def column_exists(df: DataFrame, column_name: str) -> bool:
    return column_name in df.columns


def safe_col(df: DataFrame, column_name: str, target_type: str | None = None) -> Any:
    """Return an existing column or a typed NULL if it does not exist."""
    expression = F.col(column_name) if column_exists(df, column_name) else F.lit(None)
    return expression.cast(target_type) if target_type is not None else expression


def canonical_columns(canonical_schema: Dict[str, str]) -> List[str]:
    return list(canonical_schema.keys())


def add_missing_canonical_columns(df: DataFrame, canonical_schema: Dict[str, str]) -> DataFrame:
    """Add missing canonical columns and enforce final column order."""
    result = df
    for column_name, column_type in canonical_schema.items():
        if column_name not in result.columns:
            result = result.withColumn(column_name, F.lit(None).cast(column_type))
    return result.select(*canonical_columns(canonical_schema))


def create_trip_id_expr() -> Any:
    """Create the same technical trip id used in the notebooks."""
    return F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("service_type"), F.lit("")),
            F.coalesce(F.col("pickup_datetime").cast("string"), F.lit("")),
            F.coalesce(F.col("dropoff_datetime").cast("string"), F.lit("")),
            F.coalesce(F.col("pickup_location_id").cast("string"), F.lit("")),
            F.coalesce(F.col("dropoff_location_id").cast("string"), F.lit("")),
            F.coalesce(F.col("trip_distance").cast("string"), F.lit("")),
            F.coalesce(F.col("total_amount").cast("string"), F.lit("")),
            F.coalesce(F.col("source_file"), F.lit("")),
        ),
        256,
    )


def _lit_int(value: Any) -> Any:
    try:
        return F.lit(int(value)).cast("integer") if value is not None else F.lit(None).cast("integer")
    except (TypeError, ValueError):
        return F.lit(None).cast("integer")


def homologate_yellow(
    df: DataFrame,
    source_file: str,
    year_value: Any,
    month_value: Any,
    canonical_schema: Dict[str, str] | None = None,
) -> DataFrame:
    """Map yellow taxi data to the canonical schema."""
    canonical_schema = canonical_schema or DEFAULT_CANONICAL_SCHEMA
    result = df.select(
        F.lit("yellow").alias("service_type"),
        safe_col(df, "VendorID", "long").alias("vendor_id"),
        safe_col(df, "tpep_pickup_datetime", "timestamp").alias("pickup_datetime"),
        safe_col(df, "tpep_dropoff_datetime", "timestamp").alias("dropoff_datetime"),
        safe_col(df, "passenger_count", "double").alias("passenger_count"),
        safe_col(df, "trip_distance", "double").alias("trip_distance"),
        safe_col(df, "PULocationID", "long").alias("pickup_location_id"),
        safe_col(df, "DOLocationID", "long").alias("dropoff_location_id"),
        safe_col(df, "payment_type", "long").alias("payment_type"),
        safe_col(df, "fare_amount", "double").alias("fare_amount"),
        safe_col(df, "extra", "double").alias("extra_amount"),
        safe_col(df, "mta_tax", "double").alias("mta_tax"),
        safe_col(df, "tip_amount", "double").alias("tip_amount"),
        safe_col(df, "tolls_amount", "double").alias("tolls_amount"),
        safe_col(df, "total_amount", "double").alias("total_amount"),
        safe_col(df, "congestion_surcharge", "double").alias("congestion_surcharge"),
        safe_col(df, "airport_fee", "double").alias("airport_fee"),
        _lit_int(year_value).alias("year"),
        _lit_int(month_value).alias("month"),
        F.lit(source_file).alias("source_file"),
        F.current_timestamp().alias("ingestion_timestamp"),
        F.lit("RECOVERED_SCHEMA_CANONICAL").alias("quality_status"),
    )
    return add_missing_canonical_columns(result.withColumn("trip_id", create_trip_id_expr()), canonical_schema)


def homologate_green(
    df: DataFrame,
    source_file: str,
    year_value: Any,
    month_value: Any,
    canonical_schema: Dict[str, str] | None = None,
) -> DataFrame:
    """Map green taxi data to the canonical schema."""
    canonical_schema = canonical_schema or DEFAULT_CANONICAL_SCHEMA
    result = df.select(
        F.lit("green").alias("service_type"),
        safe_col(df, "VendorID", "long").alias("vendor_id"),
        safe_col(df, "lpep_pickup_datetime", "timestamp").alias("pickup_datetime"),
        safe_col(df, "lpep_dropoff_datetime", "timestamp").alias("dropoff_datetime"),
        safe_col(df, "passenger_count", "double").alias("passenger_count"),
        safe_col(df, "trip_distance", "double").alias("trip_distance"),
        safe_col(df, "PULocationID", "long").alias("pickup_location_id"),
        safe_col(df, "DOLocationID", "long").alias("dropoff_location_id"),
        safe_col(df, "payment_type", "long").alias("payment_type"),
        safe_col(df, "fare_amount", "double").alias("fare_amount"),
        safe_col(df, "extra", "double").alias("extra_amount"),
        safe_col(df, "mta_tax", "double").alias("mta_tax"),
        safe_col(df, "tip_amount", "double").alias("tip_amount"),
        safe_col(df, "tolls_amount", "double").alias("tolls_amount"),
        safe_col(df, "total_amount", "double").alias("total_amount"),
        safe_col(df, "congestion_surcharge", "double").alias("congestion_surcharge"),
        F.lit(None).cast("double").alias("airport_fee"),
        _lit_int(year_value).alias("year"),
        _lit_int(month_value).alias("month"),
        F.lit(source_file).alias("source_file"),
        F.current_timestamp().alias("ingestion_timestamp"),
        F.lit("RECOVERED_SCHEMA_CANONICAL").alias("quality_status"),
    )
    return add_missing_canonical_columns(result.withColumn("trip_id", create_trip_id_expr()), canonical_schema)


def homologate_fhvhv(
    df: DataFrame,
    source_file: str,
    year_value: Any,
    month_value: Any,
    canonical_schema: Dict[str, str] | None = None,
) -> DataFrame:
    """Map FHVHV data to the canonical schema."""
    canonical_schema = canonical_schema or DEFAULT_CANONICAL_SCHEMA
    base_fare = safe_col(df, "base_passenger_fare", "double")
    tolls = safe_col(df, "tolls", "double")
    sales_tax = safe_col(df, "sales_tax", "double")
    total_reconstructed = (
        F.coalesce(base_fare, F.lit(0.0))
        + F.coalesce(tolls, F.lit(0.0))
        + F.coalesce(sales_tax, F.lit(0.0))
    )

    result = df.select(
        F.lit("fhvhv").alias("service_type"),
        F.lit(None).cast("long").alias("vendor_id"),
        safe_col(df, "pickup_datetime", "timestamp").alias("pickup_datetime"),
        safe_col(df, "dropoff_datetime", "timestamp").alias("dropoff_datetime"),
        F.lit(None).cast("double").alias("passenger_count"),
        safe_col(df, "trip_miles", "double").alias("trip_distance"),
        safe_col(df, "PULocationID", "long").alias("pickup_location_id"),
        safe_col(df, "DOLocationID", "long").alias("dropoff_location_id"),
        F.lit(None).cast("long").alias("payment_type"),
        base_fare.alias("fare_amount"),
        F.lit(None).cast("double").alias("extra_amount"),
        F.lit(None).cast("double").alias("mta_tax"),
        safe_col(df, "tips", "double").alias("tip_amount"),
        tolls.alias("tolls_amount"),
        total_reconstructed.cast("double").alias("total_amount"),
        F.lit(None).cast("double").alias("congestion_surcharge"),
        F.lit(None).cast("double").alias("airport_fee"),
        _lit_int(year_value).alias("year"),
        _lit_int(month_value).alias("month"),
        F.lit(source_file).alias("source_file"),
        F.current_timestamp().alias("ingestion_timestamp"),
        F.lit("RECOVERED_SCHEMA_CANONICAL").alias("quality_status"),
    )
    return add_missing_canonical_columns(result.withColumn("trip_id", create_trip_id_expr()), canonical_schema)


def reconstruct_file(
    spark: SparkSession,
    row: Row,
    process_id: str,
    canonical_schema: Dict[str, str],
) -> Tuple[Optional[DataFrame], Optional[Dict[str, Any]]]:
    """Reconstruct one readable raw file to the canonical schema."""
    service_type = _row_value(row, "service_type")
    file_path = Path(str(_row_value(row, "file_path")))
    file_name = str(_row_value(row, "file_name"))
    year_value = _row_value(row, "partition_year")
    month_value = _row_value(row, "partition_month")

    try:
        df = spark.read.parquet(spark_path(file_path))

        if service_type == "yellow":
            return homologate_yellow(df, file_name, year_value, month_value, canonical_schema), None
        if service_type == "green":
            return homologate_green(df, file_name, year_value, month_value, canonical_schema), None
        if service_type == "fhvhv":
            return homologate_fhvhv(df, file_name, year_value, month_value, canonical_schema), None

        return None, {
            "process_id": process_id,
            "file_name": file_name,
            "file_path": str(file_path),
            "service_type": str(service_type),
            "stage": "schema_reconstruction",
            "classification": NOT_RECOVERABLE_UNSUPPORTED_FORMAT,
            "technical_reason": "Service type is not supported for canonical homologation.",
            "recommended_action": "Review the source manually.",
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    except Exception as exc:  # noqa: BLE001
        return None, {
            "process_id": process_id,
            "file_name": file_name,
            "file_path": str(file_path),
            "service_type": str(service_type),
            "stage": "schema_reconstruction",
            "classification": NOT_RECOVERABLE_CORRUPT_METADATA,
            "technical_reason": clean_error_message(exc),
            "recommended_action": "Send to technical quarantine and review the original file.",
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }


def get_recoverability(problem_category: str) -> str:
    if problem_category in [
        RECUPERABLE_SCHEMA_MISMATCH,
        RECUPERABLE_MISSING_COLUMNS,
        RECUPERABLE_TYPE_CASTING,
    ]:
        return RECOVERABLE
    if problem_category == PARTIALLY_RECOVERABLE:
        return PARTIALLY_RECOVERABLE_STATUS
    return NOT_RECOVERABLE


def classify_exception(file_path: str | Path, exception_message: Any) -> str:
    """Classify unreadable files using the required technical categories."""
    file_path_text = str(file_path).lower()
    message = str(exception_message).lower()

    if Path(file_path).exists() and Path(file_path).stat().st_size == 0:
        return NOT_RECOVERABLE_EMPTY_FILE
    if not file_path_text.endswith(".parquet"):
        return NOT_RECOVERABLE_UNSUPPORTED_FORMAT

    if any(
        keyword in message
        for keyword in [
            "not a parquet",
            "parquet magic",
            "footer",
            "metadata",
            "expected magic number",
            "checksum",
            "eofexception",
            "ioexception",
        ]
    ):
        return NOT_RECOVERABLE_CORRUPT_METADATA

    if any(keyword in message for keyword in ["cannot cast", "cannot be cast", "schema conversion"]):
        return RECUPERABLE_TYPE_CASTING
    if any(keyword in message for keyword in ["cannot resolve", "missing", "not found"]):
        return RECUPERABLE_MISSING_COLUMNS
    if "schema" in message:
        return RECUPERABLE_SCHEMA_MISMATCH

    return NOT_RECOVERABLE_CORRUPT_METADATA


def find_equivalent_column(df_columns: Iterable[str], canonical_column: str) -> Optional[str]:
    """Find the real source column matching a canonical concept."""
    for name in COLUMN_EQUIVALENCES.get(canonical_column, [canonical_column]):
        if name in df_columns:
            return name
    return None


def build_total_amount_expression(df: DataFrame) -> Any:
    """Build total_amount, including FHVHV reconstruction when needed."""
    if "total_amount" in df.columns:
        return F.col("total_amount").cast("double")

    components = [
        F.coalesce(F.col(column).cast("double"), F.lit(0.0))
        for column in ["base_passenger_fare", "tolls", "sales_tax"]
        if column in df.columns
    ]
    if not components:
        return F.lit(None).cast("double")

    expression = components[0]
    for component in components[1:]:
        expression = expression + component
    return expression.cast("double")


def reconstruct_to_canonical(
    df: DataFrame,
    file_path: str | Path,
    canonical_schema: Dict[str, str] | None = None,
) -> DataFrame:
    """Generic recovery path for readable files with non-standard schemas."""
    canonical_schema = canonical_schema or DEFAULT_CANONICAL_SCHEMA
    file_path = Path(file_path)
    source_file = file_path.name
    service_type = "yellow" if "yellow" in str(file_path).lower() else "green" if "green" in str(file_path).lower() else "fhvhv" if "fhvhv" in str(file_path).lower() else "unknown"
    year_value = extract_partition_value(file_path, "year")
    month_value = extract_partition_value(file_path, "month")

    selected = []
    for canonical_column, target_type in canonical_schema.items():
        if canonical_column == "trip_id":
            continue
        if canonical_column == "service_type":
            selected.append(F.lit(service_type).alias(canonical_column))
            continue
        if canonical_column == "source_file":
            selected.append(F.lit(source_file).alias(canonical_column))
            continue
        if canonical_column == "ingestion_timestamp":
            selected.append(F.current_timestamp().alias(canonical_column))
            continue
        if canonical_column == "quality_status":
            selected.append(F.lit("RECOVERED_SCHEMA_CANONICAL").alias(canonical_column))
            continue
        if canonical_column == "year":
            selected.append(_lit_int(year_value).alias(canonical_column))
            continue
        if canonical_column == "month":
            selected.append(_lit_int(month_value).alias(canonical_column))
            continue
        if canonical_column == "total_amount":
            selected.append(build_total_amount_expression(df).alias(canonical_column))
            continue

        source_column = find_equivalent_column(df.columns, canonical_column)
        if source_column:
            selected.append(F.col(source_column).cast(target_type).alias(canonical_column))
        else:
            selected.append(F.lit(None).cast(target_type).alias(canonical_column))

    recovered = df.select(*selected)
    return add_missing_canonical_columns(recovered.withColumn("trip_id", create_trip_id_expr()), canonical_schema)


def load_latest_audit_inventory(spark: SparkSession, audit_path: str | Path) -> DataFrame:
    """Load the newest audit_file_inventory folder."""
    latest = find_latest_parquet_folder(audit_path, name_prefixes=["audit_file_inventory_"])
    if latest is None:
        raise FileNotFoundError("No audit_file_inventory output was found. Run Phase 1 first.")
    return spark.read.parquet(spark_path(latest))


def reconstruct_readable_files(
    spark: SparkSession,
    audit_file_inventory: DataFrame,
    process_id: str,
    canonical_schema: Dict[str, str],
) -> Tuple[DataFrame, DataFrame]:
    """Reconstruct all readable business source files and return bronze plus exclusions."""
    readable_rows = (
        audit_file_inventory.filter(
            (F.col("read_status") == "READ_OK") & F.col("service_type").isin("yellow", "green", "fhvhv")
        )
        .collect()
    )
    reconstructed_dfs: List[DataFrame] = []
    non_recoverable_rows: List[Dict[str, Any]] = []

    for row in readable_rows:
        reconstructed_df, error_info = reconstruct_file(spark, row, process_id, canonical_schema)
        if reconstructed_df is not None:
            reconstructed_dfs.append(reconstructed_df)
        elif error_info is not None:
            non_recoverable_rows.append(error_info)

    if not reconstructed_dfs:
        raise ValueError("No files were reconstructed. Check raw paths and Phase 1 audit.")

    bronze_trips_canonical = union_all_by_name(reconstructed_dfs).select(*canonical_columns(canonical_schema))

    if non_recoverable_rows:
        quarantine_df = spark.createDataFrame(non_recoverable_rows, schema=PHASE2_QUARANTINE_SCHEMA)
    else:
        quarantine_df = spark.createDataFrame([], schema=PHASE2_QUARANTINE_SCHEMA)

    return bronze_trips_canonical, quarantine_df


def write_bronze_by_service_source(
    bronze_df: DataFrame,
    bronze_path: str | Path,
    process_id: str,
) -> Path:
    """Write canonical Bronze by service and source file, matching the notebook output."""
    bronze_base_path = Path(bronze_path) / f"trips_canonical_{process_id}"

    for service in ["yellow", "green", "fhvhv"]:
        df_service = bronze_df.filter(F.col("service_type") == service)
        source_files = [row["source_file"] for row in df_service.select("source_file").distinct().collect()]

        for source_file in source_files:
            output_path = (
                bronze_base_path
                / f"service_type={service}"
                / f"source_file={sanitize_path_fragment(source_file)}"
            )
            df_file = df_service.filter(F.col("source_file") == source_file)
            if df_file.count() > 0:
                df_file.write.mode("overwrite").parquet(spark_path(output_path))

    return bronze_base_path


def run_schema_recovery(
    spark: SparkSession | None = None,
    config_path: str | Path | None = None,
    process_id: str | None = None,
    write_outputs: bool = True,
) -> Dict[str, Any]:
    """Run schema diagnosis and canonical reconstruction end to end."""
    config = load_config(config_path)
    spark = spark or create_spark_session("fase02_schema_recovery", config)
    process_id = process_id or create_process_id("fase2")

    audit_path = path_from_config(config, "audit_path")
    bronze_path = path_from_config(config, "bronze_path")
    quarantine_path = path_from_config(config, "quarantine_path")
    metadata_path = path_from_config(config, "metadata_path")
    ensure_directories([audit_path, bronze_path, quarantine_path])

    expected_schemas = load_expected_schemas(metadata_path)
    canonical_schema = load_canonical_schema(metadata_path)
    audit_inventory = load_latest_audit_inventory(spark, audit_path)

    readable_rows = (
        audit_inventory.filter(
            (F.col("read_status") == "READ_OK") & F.col("service_type").isin("yellow", "green", "fhvhv")
        )
        .collect()
    )
    diagnosis_df = diagnose_schemas(spark, readable_rows, expected_schemas, process_id)
    bronze_df, phase2_quarantine_df = reconstruct_readable_files(
        spark,
        audit_inventory,
        process_id,
        canonical_schema,
    )

    outputs: Dict[str, Any] = {
        "process_id": process_id,
        "schema_diagnosis": diagnosis_df,
        "bronze_trips_canonical": bronze_df,
        "schema_reconstruction_exclusions": phase2_quarantine_df,
    }

    if write_outputs:
        diagnosis_output = audit_path / f"schema_diagnosis_{process_id}"
        phase2_quarantine_output = quarantine_path / f"schema_reconstruction_exclusions_{process_id}"
        bronze_output = write_bronze_by_service_source(bronze_df, bronze_path, process_id)

        diagnosis_df.write.mode("overwrite").parquet(spark_path(diagnosis_output))
        phase2_quarantine_df.write.mode("overwrite").parquet(spark_path(phase2_quarantine_output))

        outputs["schema_diagnosis_output_path"] = diagnosis_output
        outputs["phase2_quarantine_output_path"] = phase2_quarantine_output
        outputs["bronze_output_path"] = bronze_output

    return outputs


if __name__ == "__main__":
    result = run_schema_recovery()
    print("Phase 2 completed:", result["process_id"])
