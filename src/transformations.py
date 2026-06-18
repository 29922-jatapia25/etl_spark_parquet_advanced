"""Phase 4: transformations, derived fields and Silver output."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

try:
    from .utils import (
        create_process_id,
        create_spark_session,
        ensure_directories,
        find_latest_parquet_folder,
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
        load_config,
        path_from_config,
        spark_path,
    )


REQUIRED_CANONICAL_COLUMNS = [
    "trip_id",
    "service_type",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "extra_amount",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "year",
    "month",
    "source_file",
    "ingestion_timestamp",
    "quality_status",
]


SILVER_COLUMNS = [
    "trip_id",
    "service_type",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "trip_duration_minutes",
    "passenger_count",
    "trip_distance",
    "average_speed_mph",
    "fare_per_mile",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "extra_amount",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "total_amount",
    "tip_percentage",
    "congestion_surcharge",
    "airport_fee",
    "is_airport_trip",
    "is_suspicious_trip",
    "year",
    "month",
    "source_file",
    "ingestion_timestamp",
    "processing_date",
    "quality_status",
]


PHASE04_REJECTED_COLUMNS = [
    "process_id",
    "trip_id",
    "service_type",
    "source_file",
    "rejection_stage",
    "rejection_rule",
    "technical_reason",
    "business_reason",
    "rejected_at",
]


def normalize_column_name(column_name: str) -> str:
    """Normalize source column names to snake-like lowercase names."""
    normalized = column_name.strip().lower()
    for old, new in [(" ", "_"), ("-", "_"), (".", "_"), ("/", "_"), ("(", ""), (")", "")]:
        normalized = normalized.replace(old, new)
    return normalized


def normalize_column_names(df: DataFrame) -> DataFrame:
    """Apply notebook column normalization to a DataFrame."""
    return df.select(*[F.col(column).alias(normalize_column_name(column)) for column in df.columns])


def ensure_required_columns(df: DataFrame) -> DataFrame:
    """Add minimum canonical columns with controlled NULL values."""
    result = df
    for column_name in REQUIRED_CANONICAL_COLUMNS:
        if column_name not in result.columns:
            result = result.withColumn(column_name, F.lit(None))
    return result


def cast_trip_columns(df: DataFrame) -> DataFrame:
    """Cast canonical columns to the types used by the transformation notebook."""
    return (
        df.withColumn("pickup_datetime", F.to_timestamp(F.col("pickup_datetime")))
        .withColumn("dropoff_datetime", F.to_timestamp(F.col("dropoff_datetime")))
        .withColumn("ingestion_timestamp", F.to_timestamp(F.col("ingestion_timestamp")))
        .withColumn("service_type", F.col("service_type").cast("string"))
        .withColumn("source_file", F.col("source_file").cast("string"))
        .withColumn("quality_status", F.col("quality_status").cast("string"))
        .withColumn("vendor_id", F.col("vendor_id").cast("string"))
        .withColumn("payment_type", F.col("payment_type").cast("string"))
        .withColumn("passenger_count", F.col("passenger_count").cast("double"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("pickup_location_id", F.col("pickup_location_id").cast("int"))
        .withColumn("dropoff_location_id", F.col("dropoff_location_id").cast("int"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("extra_amount", F.col("extra_amount").cast("double"))
        .withColumn("mta_tax", F.col("mta_tax").cast("double"))
        .withColumn("tip_amount", F.col("tip_amount").cast("double"))
        .withColumn("tolls_amount", F.col("tolls_amount").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("congestion_surcharge", F.col("congestion_surcharge").cast("double"))
        .withColumn("airport_fee", F.col("airport_fee").cast("double"))
        .withColumn("year", F.col("year").cast("int"))
        .withColumn("month", F.col("month").cast("int"))
    )


def add_technical_fields(df: DataFrame) -> DataFrame:
    """Fill partition, source and processing metadata fields."""
    return (
        df.withColumn("year", F.when(F.col("year").isNotNull(), F.col("year")).otherwise(F.year("pickup_datetime")))
        .withColumn("month", F.when(F.col("month").isNotNull(), F.col("month")).otherwise(F.month("pickup_datetime")))
        .withColumn(
            "ingestion_timestamp",
            F.when(F.col("ingestion_timestamp").isNotNull(), F.col("ingestion_timestamp")).otherwise(
                F.current_timestamp()
            ),
        )
        .withColumn("processing_date", F.current_date())
        .withColumn(
            "source_file",
            F.when(F.col("source_file").isNotNull(), F.col("source_file")).otherwise(F.lit("UNKNOWN_SOURCE_FILE")),
        )
        .withColumn(
            "service_type",
            F.when(F.col("service_type").isNotNull(), F.lower("service_type")).otherwise(F.lit("unknown")),
        )
    )


def add_or_validate_trip_id(df: DataFrame) -> DataFrame:
    """Generate trip_id when missing using the project technical key."""
    generated_trip_id = F.sha2(
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

    return df.withColumn(
        "trip_id",
        F.when(F.col("trip_id").isNotNull(), F.col("trip_id").cast("string")).otherwise(generated_trip_id),
    )


def add_derived_fields(df: DataFrame) -> DataFrame:
    """Calculate duration, speed, fare per mile, tip percentage and airport flag."""
    airport_location_ids = [132, 138]

    return (
        df.withColumn(
            "trip_duration_minutes",
            (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")) / F.lit(60.0),
        )
        .withColumn(
            "average_speed_mph",
            F.when(
                F.col("trip_duration_minutes") > 0,
                F.col("trip_distance") / (F.col("trip_duration_minutes") / F.lit(60.0)),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "fare_per_mile",
            F.when(F.col("trip_distance") > 0, F.col("fare_amount") / F.col("trip_distance")).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "tip_percentage",
            F.when(F.col("fare_amount") > 0, (F.col("tip_amount") / F.col("fare_amount")) * F.lit(100.0)).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "is_airport_trip",
            F.when(
                F.col("pickup_location_id").isin(airport_location_ids)
                | F.col("dropoff_location_id").isin(airport_location_ids)
                | (F.coalesce(F.col("airport_fee"), F.lit(0.0)) > 0),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
    )


def add_suspicious_flags(df: DataFrame) -> DataFrame:
    """Create individual suspicion rule columns and final is_suspicious_trip."""
    flagged = (
        df.withColumn("rule_invalid_distance", F.col("trip_distance") <= 0)
        .withColumn("rule_invalid_total_amount", F.col("total_amount") <= 0)
        .withColumn("rule_negative_fare", F.col("fare_amount") < 0)
        .withColumn("rule_invalid_duration", F.col("trip_duration_minutes") <= 0)
        .withColumn("rule_excessive_duration", F.col("trip_duration_minutes") > 480)
        .withColumn("rule_unreal_speed", F.col("average_speed_mph") > 100)
        .withColumn("rule_excessive_tip", F.col("tip_percentage") > 100)
        .withColumn("rule_pickup_after_dropoff", F.col("pickup_datetime") > F.col("dropoff_datetime"))
        .withColumn("rule_future_pickup", F.col("pickup_datetime") > F.current_timestamp())
    )

    return flagged.withColumn(
        "is_suspicious_trip",
        F.coalesce(F.col("rule_invalid_distance"), F.lit(False))
        | F.coalesce(F.col("rule_invalid_total_amount"), F.lit(False))
        | F.coalesce(F.col("rule_negative_fare"), F.lit(False))
        | F.coalesce(F.col("rule_invalid_duration"), F.lit(False))
        | F.coalesce(F.col("rule_excessive_duration"), F.lit(False))
        | F.coalesce(F.col("rule_unreal_speed"), F.lit(False))
        | F.coalesce(F.col("rule_excessive_tip"), F.lit(False))
        | F.coalesce(F.col("rule_pickup_after_dropoff"), F.lit(False))
        | F.coalesce(F.col("rule_future_pickup"), F.lit(False)),
    )


def critical_rejection_condition() -> Any:
    """Return the Phase 4 critical rejection condition."""
    return (
        F.col("pickup_datetime").isNull()
        | F.col("dropoff_datetime").isNull()
        | F.col("trip_distance").isNull()
        | F.col("fare_amount").isNull()
        | F.col("total_amount").isNull()
        | (F.col("trip_distance") <= 0)
        | (F.col("total_amount") <= 0)
        | (F.col("fare_amount") < 0)
        | (F.col("trip_duration_minutes") <= 0)
        | (F.col("trip_duration_minutes") > 480)
        | (F.col("average_speed_mph") > 100)
        | (F.col("tip_percentage") > 100)
        | (F.col("pickup_datetime") > F.col("dropoff_datetime"))
        | (F.col("pickup_datetime") > F.current_timestamp())
    )


def build_transformation_rejections(df: DataFrame, process_id: str) -> DataFrame:
    """Build Phase 4 rejected records with the principal rejection rule."""
    return (
        df.withColumn("process_id", F.lit(process_id))
        .withColumn("rejection_stage", F.lit("FASE_04_TRANSFORMACION"))
        .withColumn(
            "rejection_rule",
            F.when(F.col("pickup_datetime").isNull(), F.lit("NULL_PICKUP_DATETIME"))
            .when(F.col("dropoff_datetime").isNull(), F.lit("NULL_DROPOFF_DATETIME"))
            .when(F.col("trip_distance").isNull(), F.lit("NULL_TRIP_DISTANCE"))
            .when(F.col("fare_amount").isNull(), F.lit("NULL_FARE_AMOUNT"))
            .when(F.col("total_amount").isNull(), F.lit("NULL_TOTAL_AMOUNT"))
            .when(F.col("trip_distance") <= 0, F.lit("INVALID_TRIP_DISTANCE"))
            .when(F.col("total_amount") <= 0, F.lit("INVALID_TOTAL_AMOUNT"))
            .when(F.col("fare_amount") < 0, F.lit("NEGATIVE_FARE_AMOUNT"))
            .when(F.col("trip_duration_minutes") <= 0, F.lit("INVALID_TRIP_DURATION"))
            .when(F.col("trip_duration_minutes") > 480, F.lit("EXCESSIVE_TRIP_DURATION"))
            .when(F.col("average_speed_mph") > 100, F.lit("UNREALISTIC_SPEED"))
            .when(F.col("tip_percentage") > 100, F.lit("EXCESSIVE_TIP_PERCENTAGE"))
            .when(F.col("pickup_datetime") > F.col("dropoff_datetime"), F.lit("PICKUP_AFTER_DROPOFF"))
            .when(F.col("pickup_datetime") > F.current_timestamp(), F.lit("FUTURE_PICKUP_DATETIME"))
            .otherwise(F.lit("UNKNOWN_TRANSFORMATION_REJECTION")),
        )
        .withColumn(
            "technical_reason",
            F.concat(F.lit("Registro rechazado durante transformacion por regla: "), F.col("rejection_rule")),
        )
        .withColumn("business_reason", F.lit("El viaje no cumple condiciones minimas para analisis confiable."))
        .withColumn("rejected_at", F.current_timestamp())
    )


def split_valid_and_rejected(df: DataFrame, process_id: str) -> Tuple[DataFrame, DataFrame]:
    """Split Phase 4 candidates from critical transformation rejections."""
    condition = critical_rejection_condition()
    rejected = build_transformation_rejections(df.filter(condition), process_id)
    candidate_valid = df.filter(~condition)
    return candidate_valid, rejected


def deduplicate_valid_trips(df: DataFrame, shuffle_partitions: int = 8) -> Tuple[DataFrame, DataFrame, Dict[str, int]]:
    """Remove technical duplicates by trip_id while keeping the earliest ingestion."""
    candidate = df.repartition(shuffle_partitions, "trip_id")
    before_count = candidate.count()
    window_trip = Window.partitionBy("trip_id").orderBy(F.col("ingestion_timestamp").asc())

    with_row_number = candidate.withColumn("rn_dedup", F.row_number().over(window_trip))
    duplicate_records = with_row_number.filter(F.col("rn_dedup") > 1).drop("rn_dedup")
    clean_records = with_row_number.filter(F.col("rn_dedup") == 1).drop("rn_dedup")

    after_count = clean_records.count()
    return clean_records, duplicate_records, {
        "before_dedup_count": before_count,
        "after_dedup_count": after_count,
        "duplicate_count": before_count - after_count,
    }


def build_silver_dataset(df: DataFrame) -> DataFrame:
    """Create the final Silver dataset with required output columns."""
    transformed = df.withColumn(
        "quality_status",
        F.when(F.col("is_suspicious_trip") == True, F.lit("TRANSFORMED_SUSPICIOUS")).otherwise(
            F.lit("TRANSFORMED_VALID")
        ),
    )
    return transformed.select(*SILVER_COLUMNS)


def transform_bronze_to_silver(
    bronze_df: DataFrame,
    process_id: str,
    shuffle_partitions: int = 8,
) -> Dict[str, Any]:
    """Transform canonical Bronze data into Silver plus Phase 4 audit artifacts."""
    normalized = normalize_column_names(bronze_df)
    prepared = ensure_required_columns(normalized)
    typed = cast_trip_columns(prepared)
    enriched = add_technical_fields(typed)
    with_id = add_or_validate_trip_id(enriched)
    derived = add_derived_fields(with_id)
    flagged = add_suspicious_flags(derived)
    valid_candidates, rejected_records = split_valid_and_rejected(flagged, process_id)
    clean_records, duplicate_records, dedup_metrics = deduplicate_valid_trips(valid_candidates, shuffle_partitions)
    silver_df = build_silver_dataset(clean_records)

    audit_data = {
        "process_id": process_id,
        "phase": "FASE_04_TRANSFORMACION_DATOS",
        "bronze_input_records": bronze_df.count(),
        "rejected_records": rejected_records.count(),
        "duplicate_records": dedup_metrics["duplicate_count"],
        "silver_output_records": silver_df.count(),
        "suspicious_records": silver_df.filter(F.col("is_suspicious_trip") == True).count(),
        "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    return {
        "trips_silver": silver_df,
        "rejected_transform_records": rejected_records,
        "duplicate_records": duplicate_records,
        "phase04_audit_data": audit_data,
    }


def write_transformation_outputs(
    spark: SparkSession,
    trips_silver: DataFrame,
    rejected_records: DataFrame,
    audit_data: Dict[str, Any],
    silver_path: str | Path,
    quarantine_path: str | Path,
    audit_path: str | Path,
    process_id: str,
) -> Dict[str, Path]:
    """Write Silver, Phase 4 rejections and Phase 4 audit output."""
    silver_output_path = Path(silver_path) / f"trips_transformed_{process_id}"
    rejected_output_path = Path(quarantine_path) / f"rejected_records_phase04_{process_id}"
    audit_output_path = Path(audit_path) / f"phase04_transformation_audit_{process_id}"

    (
        trips_silver.repartition("service_type", "year", "month")
        .write.mode("overwrite")
        .partitionBy("service_type", "year", "month")
        .parquet(spark_path(silver_output_path))
    )

    (
        rejected_records.select(*PHASE04_REJECTED_COLUMNS)
        .write.mode("overwrite")
        .parquet(spark_path(rejected_output_path))
    )

    audit_with_paths = dict(audit_data)
    audit_with_paths["silver_output_path"] = str(silver_output_path)
    audit_with_paths["rejected_output_path"] = str(rejected_output_path)
    audit_df = spark.createDataFrame([audit_with_paths])
    audit_df.write.mode("overwrite").parquet(spark_path(audit_output_path))

    return {
        "silver_output_path": silver_output_path,
        "rejected_output_path": rejected_output_path,
        "audit_output_path": audit_output_path,
    }


def load_latest_bronze(spark: SparkSession, bronze_path: str | Path) -> DataFrame:
    """Read the latest canonical Bronze output."""
    latest_bronze = find_latest_parquet_folder(bronze_path, name_prefixes=["trips_canonical_"])
    if latest_bronze is None:
        raise FileNotFoundError("No canonical Bronze dataset was found. Run Phase 2 first.")
    return spark.read.option("recursiveFileLookup", "true").parquet(spark_path(latest_bronze))


def run_transformations(
    spark: SparkSession | None = None,
    config_path: str | Path | None = None,
    process_id: str | None = None,
    write_outputs: bool = True,
) -> Dict[str, Any]:
    """Run Phase 4 transformations end to end."""
    config = load_config(config_path)
    spark = spark or create_spark_session("fase04_transformacion", config)
    process_id = process_id or create_process_id("fase4")

    bronze_path = path_from_config(config, "bronze_path")
    silver_path = path_from_config(config, "silver_path")
    quarantine_path = path_from_config(config, "quarantine_path")
    audit_path = path_from_config(config, "audit_path")
    ensure_directories([silver_path, quarantine_path, audit_path])

    bronze_df = load_latest_bronze(spark, bronze_path)
    result = transform_bronze_to_silver(
        bronze_df,
        process_id,
        shuffle_partitions=int(config.get("spark_shuffle_partitions", 8)),
    )
    result["process_id"] = process_id

    if write_outputs:
        result["output_paths"] = write_transformation_outputs(
            spark,
            result["trips_silver"],
            result["rejected_transform_records"],
            result["phase04_audit_data"],
            silver_path,
            quarantine_path,
            audit_path,
            process_id,
        )

    return result


if __name__ == "__main__":
    output = run_transformations()
    print("Phase 4 completed:", output["process_id"])
