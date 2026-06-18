"""Phase 5: data quality validation, rejected records and metrics."""

from __future__ import annotations

from functools import reduce
from pathlib import Path
from typing import Any, Dict, List, Tuple

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


QUALITY_REQUIRED_COLUMNS = {
    "process_id": "string",
    "trip_id": "string",
    "service_type": "string",
    "source_file": "string",
    "pickup_datetime": "timestamp",
    "dropoff_datetime": "timestamp",
    "pickup_location_id": "int",
    "dropoff_location_id": "int",
    "trip_distance": "double",
    "fare_amount": "double",
    "total_amount": "double",
    "tip_amount": "double",
    "trip_duration_minutes": "double",
    "average_speed_mph": "double",
    "fare_per_mile": "double",
    "tip_percentage": "double",
    "is_suspicious_trip": "boolean",
    "year": "int",
    "month": "int",
    "quality_status": "string",
    "processing_date": "date",
}


QUALITY_REJECTED_COLUMNS = [
    "process_id",
    "trip_id",
    "service_type",
    "source_file",
    "rejection_stage",
    "rejection_rule",
    "rejection_column",
    "original_value",
    "technical_reason",
    "business_reason",
    "rejected_at",
]


QUALITY_METRICS_COLUMNS = [
    "process_id",
    "service_type",
    "year",
    "month",
    "total_records",
    "valid_records",
    "rejected_records",
    "duplicate_records",
    "null_critical_records",
    "suspicious_records",
    "quality_percentage",
    "processed_at",
]


QUALITY_REJECTED_SCHEMA = StructType(
    [
        StructField("process_id", StringType(), True),
        StructField("trip_id", StringType(), True),
        StructField("service_type", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("rejection_stage", StringType(), True),
        StructField("rejection_rule", StringType(), True),
        StructField("rejection_column", StringType(), True),
        StructField("original_value", StringType(), True),
        StructField("technical_reason", StringType(), True),
        StructField("business_reason", StringType(), True),
        StructField("rejected_at", TimestampType(), True),
    ]
)


def standardize_quality_input(df: DataFrame, process_id: str) -> DataFrame:
    """Ensure quality validation has all required columns with the correct types."""
    result = df
    for column_name, data_type in QUALITY_REQUIRED_COLUMNS.items():
        if column_name not in result.columns:
            result = result.withColumn(column_name, F.lit(None).cast(data_type))
        else:
            result = result.withColumn(column_name, F.col(column_name).cast(data_type))

    return result.withColumn(
        "process_id",
        F.when(F.col("process_id").isNull(), F.lit(process_id)).otherwise(F.col("process_id")),
    )


def build_rejection_df(
    df: DataFrame,
    condition: Any,
    rejection_rule: str,
    rejection_column: str,
    original_value_expr: Any,
    technical_reason: str,
    business_reason: str,
) -> DataFrame:
    """Build a rejected-record DataFrame for one quality rule."""
    return (
        df.filter(condition)
        .select(
            F.col("process_id").cast("string").alias("process_id"),
            F.col("trip_id").cast("string").alias("trip_id"),
            F.col("service_type").cast("string").alias("service_type"),
            F.col("source_file").cast("string").alias("source_file"),
            F.lit("FASE_05_VALIDACION_CALIDAD").alias("rejection_stage"),
            F.lit(rejection_rule).alias("rejection_rule"),
            F.lit(rejection_column).alias("rejection_column"),
            original_value_expr.cast("string").alias("original_value"),
            F.lit(technical_reason).alias("technical_reason"),
            F.lit(business_reason).alias("business_reason"),
            F.current_timestamp().alias("rejected_at"),
        )
    )


def build_quality_rule_dfs(quality_input: DataFrame) -> Dict[str, DataFrame]:
    """Create all rejection DataFrames required by the case study."""
    critical_columns = [
        "trip_id",
        "service_type",
        "pickup_datetime",
        "dropoff_datetime",
        "trip_distance",
        "pickup_location_id",
        "dropoff_location_id",
        "total_amount",
        "source_file",
    ]

    null_critical_condition = reduce(
        lambda left, right: left | right,
        [F.col(column).isNull() for column in critical_columns],
    )
    null_fields_expr = F.concat_ws(
        ",",
        *[F.when(F.col(column).isNull(), F.lit(column)).otherwise(F.lit(None)) for column in critical_columns],
    )
    rejected_nulls = build_rejection_df(
        quality_input,
        null_critical_condition,
        "NULL_CRITICAL_FIELDS",
        "critical_columns",
        null_fields_expr,
        "Existen valores nulos en campos criticos requeridos para analisis.",
        "El viaje no puede considerarse confiable si faltan identificadores, fechas, ubicacion, monto o archivo fuente.",
    )

    invalid_date_condition = (
        F.col("pickup_datetime").isNull()
        | F.col("dropoff_datetime").isNull()
        | (F.col("pickup_datetime") > F.col("dropoff_datetime"))
        | (F.col("pickup_datetime") > F.current_timestamp())
        | (F.year("pickup_datetime") < F.lit(2000))
        | (F.year("pickup_datetime") > F.year(F.current_timestamp()))
    )
    rejected_dates = build_rejection_df(
        quality_input,
        invalid_date_condition,
        "INVALID_DATE_RANGE",
        "pickup_datetime/dropoff_datetime",
        F.concat_ws(" | ", F.col("pickup_datetime"), F.col("dropoff_datetime")),
        "La fecha de recogida o llegada es nula, futura, invertida o fuera del rango esperado.",
        "Un viaje con fechas invalidas altera calculos de duracion, demanda y analisis temporal.",
    )

    invalid_amount_condition = (
        F.col("total_amount").isNull()
        | (F.col("total_amount") <= 0)
        | (F.col("fare_amount") < 0)
        | (F.col("tip_amount") < 0)
    )
    rejected_amounts = build_rejection_df(
        quality_input,
        invalid_amount_condition,
        "INVALID_AMOUNT_RANGE",
        "total_amount/fare_amount/tip_amount",
        F.concat_ws(" | ", F.col("total_amount"), F.col("fare_amount"), F.col("tip_amount")),
        "Existen montos nulos, negativos o total pagado menor o igual a cero.",
        "Los montos invalidos afectan ingresos, propinas y cualquier indicador financiero.",
    )

    invalid_distance_condition = (
        F.col("trip_distance").isNull()
        | (F.col("trip_distance") <= 0)
        | (F.col("trip_distance") > 300)
    )
    rejected_distance = build_rejection_df(
        quality_input,
        invalid_distance_condition,
        "INVALID_DISTANCE_RANGE",
        "trip_distance",
        F.col("trip_distance"),
        "La distancia del viaje es nula, menor o igual a cero, o supera un umbral operativo razonable.",
        "Una distancia invalida afecta velocidad, tarifa por milla y analisis de movilidad.",
    )

    invalid_duration_condition = (
        F.col("trip_duration_minutes").isNull()
        | (F.col("trip_duration_minutes") <= 0)
        | (F.col("trip_duration_minutes") > 480)
    )
    rejected_duration = build_rejection_df(
        quality_input,
        invalid_duration_condition,
        "INVALID_DURATION_RANGE",
        "trip_duration_minutes",
        F.col("trip_duration_minutes"),
        "La duracion del viaje es nula, negativa, cero o mayor a 480 minutos.",
        "Una duracion invalida distorsiona tiempos promedio, velocidad y eficiencia operacional.",
    )

    technical_duplicate_columns = [
        "service_type",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_distance",
        "total_amount",
        "source_file",
    ]
    quality_with_duplicates = (
        quality_input.withColumn("trip_id_duplicate_count", F.count("*").over(Window.partitionBy("trip_id")))
        .withColumn("technical_duplicate_count", F.count("*").over(Window.partitionBy(*technical_duplicate_columns)))
    )
    duplicate_condition = (
        (F.col("trip_id_duplicate_count") > 1)
        | (F.col("technical_duplicate_count") > 1)
    )
    rejected_duplicates = build_rejection_df(
        quality_with_duplicates,
        duplicate_condition,
        "TECHNICAL_DUPLICATE_RECORD",
        "trip_id/technical_key",
        F.concat_ws(" | ", F.col("trip_id"), F.col("trip_id_duplicate_count"), F.col("technical_duplicate_count")),
        "El registro aparece duplicado por trip_id o por llave tecnica del viaje.",
        "Los duplicados inflan viajes, ingresos y metricas operativas.",
    )

    invalid_partition_condition = (
        F.col("year").isNull()
        | F.col("month").isNull()
        | (F.col("year") < 2000)
        | (F.col("year") > F.year(F.current_timestamp()))
        | (F.col("month") < 1)
        | (F.col("month") > 12)
    )
    rejected_partition_integrity = build_rejection_df(
        quality_input,
        invalid_partition_condition,
        "INVALID_PARTITION_VALUES",
        "year/month",
        F.concat_ws(" | ", F.col("year"), F.col("month")),
        "Las columnas de particion year/month son nulas o estan fuera de rango.",
        "La particion invalida impide organizar correctamente los datos por periodo.",
    )

    partition_date_mismatch_condition = (
        F.col("pickup_datetime").isNotNull()
        & F.col("year").isNotNull()
        & F.col("month").isNotNull()
        & ((F.col("year") != F.year("pickup_datetime")) | (F.col("month") != F.month("pickup_datetime")))
    )
    rejected_partition_consistency = build_rejection_df(
        quality_input,
        partition_date_mismatch_condition,
        "PARTITION_DATE_MISMATCH",
        "year/month/pickup_datetime",
        F.concat_ws(" | ", F.col("year"), F.col("month"), F.col("pickup_datetime")),
        "La particion year/month no coincide con la fecha real de pickup_datetime.",
        "La inconsistencia de particion afecta consultas por periodo y reportes mensuales.",
    )

    suspicious_condition = (
        (F.col("is_suspicious_trip") == True)
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
    rejected_suspicious = build_rejection_df(
        quality_input,
        suspicious_condition,
        "SUSPICIOUS_TRIP_RULES",
        "is_suspicious_trip",
        F.concat_ws(
            " | ",
            F.col("trip_distance"),
            F.col("total_amount"),
            F.col("fare_amount"),
            F.col("trip_duration_minutes"),
            F.col("average_speed_mph"),
            F.col("tip_percentage"),
        ),
        "El viaje cumple una o mas reglas avanzadas de sospecha.",
        "Los viajes sospechosos requieren revision antes de usarse en analisis de negocio.",
    )

    outlier_condition = build_outlier_condition(quality_input)
    rejected_outliers = build_rejection_df(
        quality_input,
        outlier_condition,
        "STATISTICAL_OUTLIER_P99",
        "trip_distance/total_amount/trip_duration_minutes",
        F.concat_ws(" | ", F.col("trip_distance"), F.col("total_amount"), F.col("trip_duration_minutes")),
        "El registro supera el percentil 99 en una o mas variables numericas relevantes.",
        "Los outliers pueden distorsionar promedios, ingresos y analisis operativo.",
    )

    return {
        "rejected_nulls": rejected_nulls,
        "rejected_dates": rejected_dates,
        "rejected_amounts": rejected_amounts,
        "rejected_distance": rejected_distance,
        "rejected_duration": rejected_duration,
        "rejected_duplicates": rejected_duplicates,
        "rejected_partition_integrity": rejected_partition_integrity,
        "rejected_partition_consistency": rejected_partition_consistency,
        "rejected_suspicious": rejected_suspicious,
        "rejected_outliers": rejected_outliers,
    }


def build_outlier_condition(quality_input: DataFrame) -> Any:
    """Build the P99 outlier condition for relevant numeric fields."""
    outlier_condition = F.lit(False)
    for column in ["trip_distance", "total_amount", "trip_duration_minutes"]:
        try:
            quantile_values = quality_input.select(column).na.drop().approxQuantile(column, [0.99], 0.01)
            threshold = quantile_values[0] if quantile_values else None
        except Exception:  # noqa: BLE001 - outlier detection should not stop validation.
            threshold = None

        if threshold is not None and threshold > 0:
            outlier_condition = outlier_condition | (F.col(column) > F.lit(threshold))

    return outlier_condition


def normalize_rejected_df(df: DataFrame) -> DataFrame:
    """Ensure a rejected-record DataFrame has the required columns."""
    result = df
    for column_name in QUALITY_REJECTED_COLUMNS:
        if column_name not in result.columns:
            if column_name == "rejected_at":
                result = result.withColumn(column_name, F.current_timestamp())
            else:
                result = result.withColumn(column_name, F.lit(None).cast(StringType()))
    return result.select(*QUALITY_REJECTED_COLUMNS)


def build_quality_rejected_records(rule_dfs: Dict[str, DataFrame]) -> DataFrame:
    """Union all rejection rules into quality_rejected_records."""
    normalized = [normalize_rejected_df(df) for df in rule_dfs.values()]
    quality_rejected_records = reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        normalized,
    )
    return quality_rejected_records.dropDuplicates(["trip_id", "rejection_rule", "rejection_column"]).persist()


def split_valid_invalid_trips(
    quality_input: DataFrame,
    quality_rejected_records: DataFrame,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Split quality input into valid and invalid trips using rejected trip ids."""
    rejected_trip_ids = (
        quality_rejected_records.select("trip_id")
        .where(F.col("trip_id").isNotNull())
        .dropDuplicates()
    )

    valid_trips = quality_input.join(rejected_trip_ids, on="trip_id", how="left_anti").withColumn(
        "quality_status", F.lit("VALID")
    )
    invalid_trips = quality_input.join(rejected_trip_ids, on="trip_id", how="inner").withColumn(
        "quality_status", F.lit("REJECTED")
    )
    return valid_trips, invalid_trips, rejected_trip_ids


def build_quality_metrics_summary(
    quality_input: DataFrame,
    valid_trips: DataFrame,
    invalid_trips: DataFrame,
    rule_dfs: Dict[str, DataFrame],
    process_id: str,
) -> DataFrame:
    """Create quality_metrics_summary grouped by service, year and month."""
    group_cols = ["service_type", "year", "month"]

    base_counts = quality_input.groupBy(*group_cols).agg(F.count("*").alias("total_records"))
    valid_counts = valid_trips.groupBy(*group_cols).agg(F.count("*").alias("valid_records"))
    rejected_counts = invalid_trips.groupBy(*group_cols).agg(F.count("*").alias("rejected_records"))

    duplicate_counts = (
        rule_dfs["rejected_duplicates"]
        .select("trip_id")
        .dropDuplicates()
        .join(quality_input, on="trip_id", how="inner")
        .groupBy(*group_cols)
        .agg(F.countDistinct("trip_id").alias("duplicate_records"))
    )
    null_critical_counts = (
        rule_dfs["rejected_nulls"]
        .select("trip_id")
        .dropDuplicates()
        .join(quality_input, on="trip_id", how="inner")
        .groupBy(*group_cols)
        .agg(F.countDistinct("trip_id").alias("null_critical_records"))
    )
    suspicious_counts = (
        quality_input.where(F.col("is_suspicious_trip") == True)
        .groupBy(*group_cols)
        .agg(F.count("*").alias("suspicious_records"))
    )

    return (
        base_counts.join(valid_counts, on=group_cols, how="left")
        .join(rejected_counts, on=group_cols, how="left")
        .join(duplicate_counts, on=group_cols, how="left")
        .join(null_critical_counts, on=group_cols, how="left")
        .join(suspicious_counts, on=group_cols, how="left")
        .na.fill(
            {
                "valid_records": 0,
                "rejected_records": 0,
                "duplicate_records": 0,
                "null_critical_records": 0,
                "suspicious_records": 0,
            }
        )
        .withColumn("process_id", F.lit(process_id))
        .withColumn(
            "quality_percentage",
            F.when(F.col("total_records") > 0, F.round((F.col("valid_records") / F.col("total_records")) * 100, 2))
            .otherwise(F.lit(0.0)),
        )
        .withColumn("processed_at", F.current_timestamp())
        .select(*QUALITY_METRICS_COLUMNS)
        .orderBy("year", "month", "service_type")
    )


def validate_quality_outputs(
    quality_rejected_records: DataFrame,
    quality_metrics_summary: DataFrame,
) -> None:
    """Fail fast if mandatory quality outputs are missing columns."""
    missing_rejected = [col for col in QUALITY_REJECTED_COLUMNS if col not in quality_rejected_records.columns]
    missing_metrics = [col for col in QUALITY_METRICS_COLUMNS if col not in quality_metrics_summary.columns]
    if missing_rejected or missing_metrics:
        raise ValueError(
            "Missing quality output columns. "
            f"quality_rejected_records={missing_rejected}; "
            f"quality_metrics_summary={missing_metrics}"
        )


def validate_quality(
    silver_df: DataFrame,
    process_id: str,
) -> Dict[str, Any]:
    """Run quality validation and return rejected, metrics and valid trips."""
    quality_input = standardize_quality_input(silver_df, process_id)
    rule_dfs = build_quality_rule_dfs(quality_input)
    quality_rejected_records = build_quality_rejected_records(rule_dfs)
    valid_trips, invalid_trips, rejected_trip_ids = split_valid_invalid_trips(
        quality_input,
        quality_rejected_records,
    )
    quality_metrics_summary = build_quality_metrics_summary(
        quality_input,
        valid_trips,
        invalid_trips,
        rule_dfs,
        process_id,
    )
    validate_quality_outputs(quality_rejected_records, quality_metrics_summary)

    return {
        "quality_input": quality_input,
        "quality_rejected_records": quality_rejected_records,
        "quality_metrics_summary": quality_metrics_summary,
        "valid_trips": valid_trips,
        "invalid_trips": invalid_trips,
        "rejected_trip_ids": rejected_trip_ids,
        "rule_dfs": rule_dfs,
    }


def write_quality_outputs(
    quality_rejected_records: DataFrame,
    quality_metrics_summary: DataFrame,
    valid_trips: DataFrame,
    silver_path: str | Path,
    quarantine_path: str | Path,
    audit_path: str | Path,
    process_id: str,
) -> Dict[str, Path]:
    """Write Phase 5 outputs to quarantine, audit and validated Silver."""
    quality_rejected_output_path = Path(quarantine_path) / f"quality_rejected_records_{process_id}"
    quality_metrics_output_path = Path(audit_path) / f"quality_metrics_summary_{process_id}"
    validated_output_path = Path(silver_path) / f"validated_trips_{process_id}"

    (
        quality_rejected_records.repartition(4)
        .write.mode("overwrite")
        .parquet(spark_path(quality_rejected_output_path))
    )
    (
        quality_metrics_summary.coalesce(1)
        .write.mode("overwrite")
        .parquet(spark_path(quality_metrics_output_path))
    )
    (
        valid_trips.repartition("service_type", "year", "month")
        .write.mode("overwrite")
        .partitionBy("service_type", "year", "month")
        .parquet(spark_path(validated_output_path))
    )

    return {
        "quality_rejected_output_path": quality_rejected_output_path,
        "quality_metrics_output_path": quality_metrics_output_path,
        "validated_output_path": validated_output_path,
    }


def load_latest_silver_for_quality(spark: SparkSession, silver_path: str | Path) -> DataFrame:
    """Read the latest transformed Silver output, excluding quality subfolders."""
    latest_silver = find_latest_parquet_folder(
        silver_path,
        name_prefixes=["trips_transformed_"],
        excluded_keywords=[
            "_temporary",
            "quality_rejected_records",
            "quality_metrics_summary",
            "rejected",
            "quarantine",
            "metrics",
            "audit",
        ],
    )
    if latest_silver is None:
        raise FileNotFoundError("No transformed Silver dataset was found. Run Phase 4 first.")
    return spark.read.parquet(spark_path(latest_silver))


def run_quality_validation(
    spark: SparkSession | None = None,
    config_path: str | Path | None = None,
    process_id: str | None = None,
    write_outputs: bool = True,
) -> Dict[str, Any]:
    """Run Phase 5 quality validation end to end."""
    config = load_config(config_path)
    spark = spark or create_spark_session("fase05_validacion_calidad", config)
    process_id = process_id or create_process_id("fase5")

    silver_path = path_from_config(config, "silver_path")
    quarantine_path = path_from_config(config, "quarantine_path")
    audit_path = path_from_config(config, "audit_path")
    ensure_directories([silver_path, quarantine_path, audit_path])

    silver_df = load_latest_silver_for_quality(spark, silver_path)
    result = validate_quality(silver_df, process_id)
    result["process_id"] = process_id

    if write_outputs:
        result["output_paths"] = write_quality_outputs(
            result["quality_rejected_records"],
            result["quality_metrics_summary"],
            result["valid_trips"],
            silver_path,
            quarantine_path,
            audit_path,
            process_id,
        )

    return result


if __name__ == "__main__":
    output = run_quality_validation()
    print("Phase 5 completed:", output["process_id"])
