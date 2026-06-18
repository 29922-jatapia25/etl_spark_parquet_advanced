"""Phase 1: Parquet extraction, file audit and technical quarantine."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

try:
    from .utils import (
        clean_error_message,
        create_process_id,
        create_spark_session,
        ensure_directories,
        extract_partition_value,
        file_size_mb,
        list_real_parquet_files,
        load_config,
        path_from_config,
        schema_hash,
        spark_path,
    )
except ImportError:  # pragma: no cover - allows running as python src/extract.py
    from utils import (  # type: ignore
        clean_error_message,
        create_process_id,
        create_spark_session,
        ensure_directories,
        extract_partition_value,
        file_size_mb,
        list_real_parquet_files,
        load_config,
        path_from_config,
        schema_hash,
        spark_path,
    )


AUDIT_FILE_INVENTORY_SCHEMA = StructType(
    [
        StructField("process_id", StringType(), False),
        StructField("source_system", StringType(), True),
        StructField("service_type", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("file_size_mb", DoubleType(), True),
        StructField("partition_year", IntegerType(), True),
        StructField("partition_month", IntegerType(), True),
        StructField("read_status", StringType(), True),
        StructField("record_count", LongType(), True),
        StructField("column_count", IntegerType(), True),
        StructField("schema_hash", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("processed_at", TimestampType(), True),
    ]
)


FILE_QUARANTINE_SCHEMA = StructType(
    [
        StructField("process_id", StringType(), False),
        StructField("source_system", StringType(), True),
        StructField("service_type", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("rejection_stage", StringType(), True),
        StructField("error_category", StringType(), True),
        StructField("technical_reason", StringType(), True),
        StructField("recommended_action", StringType(), True),
        StructField("rejected_at", TimestampType(), True),
    ]
)


def detect_service_type(file_path: str | Path) -> str:
    """Infer service type from the path used in the project raw layout."""
    parts = [part.lower() for part in Path(file_path).parts]

    if "yellow" in parts:
        return "yellow"
    if "green" in parts:
        return "green"
    if "fhvhv" in parts:
        return "fhvhv"
    if "bad_parquet" in parts:
        return "bad_parquet"

    return "unknown"


def detect_source_system(service_type: str, config: Dict[str, Any]) -> str:
    """Map service type to the source system name used in audit tables."""
    if service_type == "bad_parquet":
        return config.get("source_system_bad", "APACHE_PARQUET_TESTING")
    return config.get("source_system_nyc", "NYC_TLC")


def classify_file_error(file_path: str | Path, error_message: Optional[str]) -> Optional[str]:
    """Classify read failures using the categories required by the case study."""
    path = Path(file_path)

    if path.exists() and path.stat().st_size == 0:
        return "NOT_RECOVERABLE_EMPTY_FILE"

    if error_message is None:
        return None

    error_text = str(error_message).lower()

    if "not a parquet" in error_text or "unsupported" in error_text:
        return "NOT_RECOVERABLE_UNSUPPORTED_FORMAT"

    corrupt_terms = ["corrupt", "magic", "footer", "metadata", "eof", "checksum"]
    if any(term in error_text for term in corrupt_terms):
        return "NOT_RECOVERABLE_CORRUPT_METADATA"

    type_terms = ["cannot be cast", "column cannot be converted", "schema conversion"]
    if any(term in error_text for term in type_terms):
        return "RECUPERABLE_TYPE_CASTING"

    return "PARTIALLY_RECOVERABLE"


def build_file_manifest(raw_path: str | Path, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build the raw file manifest before Spark reads the data."""
    rows = []
    for file_path in list_real_parquet_files(raw_path):
        service_type = detect_service_type(file_path)
        rows.append(
            {
                "source_system": detect_source_system(service_type, config),
                "service_type": service_type,
                "file_name": file_path.name,
                "file_path": spark_path(file_path),
                "file_size_mb": file_size_mb(file_path),
                "partition_year": extract_partition_value(file_path, "year"),
                "partition_month": extract_partition_value(file_path, "month"),
            }
        )
    return rows


def read_partitioned_folder_by_files(
    spark: SparkSession,
    folder_path: str | Path,
) -> tuple[DataFrame, List[Path]]:
    """Read a partitioned folder by passing Spark the real Parquet files."""
    parquet_files = list_real_parquet_files(folder_path)
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files were found in: {folder_path}")

    return spark.read.parquet(*[spark_path(file) for file in parquet_files]), parquet_files


def build_audit_file_inventory(
    spark: SparkSession,
    raw_path: str | Path,
    process_id: str,
    config: Dict[str, Any],
) -> DataFrame:
    """Read every raw Parquet file independently and create audit_file_inventory."""
    inventory_rows: List[Dict[str, Any]] = []
    parquet_files = list_real_parquet_files(raw_path)

    for file_path in parquet_files:
        service_type = detect_service_type(file_path)
        source_system = detect_source_system(service_type, config)
        processed_at = datetime.now()

        try:
            if Path(file_path).stat().st_size == 0:
                raise ValueError("Empty file: size is 0 bytes")

            df_tmp = spark.read.parquet(spark_path(file_path))
            record_count = df_tmp.count()
            column_count = len(df_tmp.columns)
            hash_value = schema_hash(df_tmp.schema)
            read_status = "READ_OK"
            error_message = None
        except Exception as exc:  # noqa: BLE001 - audit must capture all read failures.
            record_count = None
            column_count = None
            hash_value = None
            read_status = "READ_ERROR"
            error_message = clean_error_message(exc, max_length=1000)

        inventory_rows.append(
            {
                "process_id": process_id,
                "source_system": source_system,
                "service_type": service_type,
                "file_name": file_path.name,
                "file_path": spark_path(file_path),
                "file_size_mb": file_size_mb(file_path),
                "partition_year": extract_partition_value(file_path, "year"),
                "partition_month": extract_partition_value(file_path, "month"),
                "read_status": read_status,
                "record_count": record_count,
                "column_count": column_count,
                "schema_hash": hash_value,
                "error_message": error_message,
                "processed_at": processed_at,
            }
        )

    return spark.createDataFrame(inventory_rows, schema=AUDIT_FILE_INVENTORY_SCHEMA)


def build_file_quarantine_references(
    spark: SparkSession,
    audit_file_inventory: DataFrame,
) -> DataFrame:
    """Create quarantine rows for files that failed during extraction."""
    failed_files = audit_file_inventory.filter(F.col("read_status") != "READ_OK").collect()
    quarantine_rows: List[Dict[str, Any]] = []

    for row in failed_files:
        error_category = classify_file_error(row["file_path"], row["error_message"])
        quarantine_rows.append(
            {
                "process_id": row["process_id"],
                "source_system": row["source_system"],
                "service_type": row["service_type"],
                "file_name": row["file_name"],
                "file_path": row["file_path"],
                "rejection_stage": "EXTRACTION",
                "error_category": error_category,
                "technical_reason": row["error_message"],
                "recommended_action": (
                    "Review the source file, validate Parquet format and decide "
                    "whether it must be replaced or excluded."
                ),
                "rejected_at": datetime.now(),
            }
        )

    return spark.createDataFrame(quarantine_rows, schema=FILE_QUARANTINE_SCHEMA)


def validate_audit_inventory(audit_file_inventory: DataFrame, quarantine_files: DataFrame) -> Dict[str, int]:
    """Return acceptance metrics for Phase 1."""
    return {
        "total_files": audit_file_inventory.count(),
        "read_ok_files": audit_file_inventory.filter(F.col("read_status") == "READ_OK").count(),
        "read_error_files": audit_file_inventory.filter(F.col("read_status") != "READ_OK").count(),
        "quarantine_files": quarantine_files.count(),
        "distinct_schemas": (
            audit_file_inventory.filter(F.col("schema_hash").isNotNull())
            .select("schema_hash")
            .distinct()
            .count()
        ),
    }


def run_extraction(
    spark: SparkSession | None = None,
    config_path: str | Path | None = None,
    process_id: str | None = None,
    write_outputs: bool = True,
) -> Dict[str, Any]:
    """Run Phase 1 end to end and optionally write audit/quarantine outputs."""
    config = load_config(config_path)
    spark = spark or create_spark_session("fase01_extraccion", config)
    process_id = process_id or create_process_id("fase1")

    raw_path = path_from_config(config, "raw_path")
    audit_path = path_from_config(config, "audit_path")
    quarantine_path = path_from_config(config, "quarantine_path")
    ensure_directories([audit_path, quarantine_path])

    audit_df = build_audit_file_inventory(spark, raw_path, process_id, config)
    quarantine_df = build_file_quarantine_references(spark, audit_df)
    metrics = validate_audit_inventory(audit_df, quarantine_df)

    outputs: Dict[str, Any] = {
        "process_id": process_id,
        "audit_file_inventory": audit_df,
        "quarantine_file_references": quarantine_df,
        "metrics": metrics,
    }

    if write_outputs:
        audit_output = audit_path / f"audit_file_inventory_{process_id}"
        quarantine_output = quarantine_path / f"quarantine_file_references_{process_id}"

        audit_df.write.mode("overwrite").parquet(spark_path(audit_output))
        quarantine_df.write.mode("overwrite").parquet(spark_path(quarantine_output))

        outputs["audit_output_path"] = audit_output
        outputs["quarantine_output_path"] = quarantine_output

    return outputs


if __name__ == "__main__":
    result = run_extraction()
    print("Phase 1 completed:", result["process_id"])
    print(result["metrics"])
