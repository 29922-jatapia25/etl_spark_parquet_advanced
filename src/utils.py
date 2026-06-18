"""Shared utilities for the PySpark Lakehouse ETL pipeline.

This module keeps path handling, configuration loading and Spark setup in one
place so the phase modules stay focused on ETL logic.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from datetime import datetime
from functools import reduce
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "etl_config.yaml"


DEFAULT_RELATIVE_PATHS = {
    "raw_path": "data/raw",
    "audit_path": "data/audit",
    "quarantine_path": "data/quarantine",
    "bronze_path": "data/bronze",
    "silver_path": "data/silver",
    "gold_path": "data/gold",
    "metadata_path": "metadata",
    "database_path": "data/database/etl_taxi_gold.db",
}


def spark_path(path: str | Path) -> str:
    """Return an absolute path with separators accepted by Spark on Windows."""
    return str(Path(path).resolve()).replace("\\", "/")


def load_config(config_path: str | Path | None = None) -> Dict[str, Any]:
    """Load YAML config and fill missing project paths with sane defaults."""
    path = Path(config_path) if config_path is not None else DEFAULT_CONFIG_PATH
    config: Dict[str, Any] = {}

    if path.exists():
        with path.open("r", encoding="utf-8") as file:
            config = yaml.safe_load(file) or {}

    for key, relative_path in DEFAULT_RELATIVE_PATHS.items():
        config.setdefault(key, str(PROJECT_ROOT / relative_path))

    config.setdefault("project_name", PROJECT_ROOT.name)
    config.setdefault("source_system_nyc", "NYC_TLC")
    config.setdefault("source_system_bad", "APACHE_PARQUET_TESTING")
    config.setdefault("database_engine", "sqlite")
    config.setdefault("read_mode", "individual_files")
    config.setdefault("process_all_files", True)
    config.setdefault("spark_shuffle_partitions", 8)

    return config


def path_from_config(config: Dict[str, Any], key: str) -> Path:
    """Resolve a configured path to a Path object."""
    value = config.get(key)
    if value is None:
        raise KeyError(f"Missing path in config: {key}")
    return Path(value)


def create_process_id(prefix: str = "run") -> str:
    """Create a deterministic-looking process id for a pipeline execution."""
    return f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def configure_local_environment(hadoop_home: str = r"C:\hadoop") -> None:
    """Configure Python and Hadoop environment variables used by local Spark."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    if Path(hadoop_home).exists():
        os.environ.setdefault("HADOOP_HOME", hadoop_home)
        os.environ.setdefault("hadoop.home.dir", hadoop_home)
        hadoop_bin = str(Path(hadoop_home) / "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")


def create_spark_session(
    app_name: str = "etl_spark_parquet_advanced",
    config: Dict[str, Any] | None = None,
) -> SparkSession:
    """Create a local Spark session with the project defaults."""
    config = config or {}
    configure_local_environment(config.get("hadoop_home", r"C:\hadoop"))

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.shuffle.partitions", str(config.get("spark_shuffle_partitions", 8)))
    )

    master = config.get("spark_master")
    if master:
        builder = builder.master(master)

    return builder.getOrCreate()


def clean_error_message(error: Any, max_length: int = 1500) -> Optional[str]:
    """Normalize exception text for audit/quarantine tables."""
    if error is None:
        return None
    text = str(error).replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_length]


def schema_hash(schema: Any, mode: str = "json") -> str:
    """Create a hash for a Spark schema."""
    if mode == "simple":
        schema_text = schema.simpleString()
    else:
        schema_text = schema.json()
    return hashlib.md5(schema_text.encode("utf-8")).hexdigest()


def load_json(path: str | Path) -> Any:
    """Read a JSON file using UTF-8."""
    with Path(path).open("r", encoding="utf-8") as file:
        return json.load(file)


def write_json(path: str | Path, payload: Any) -> None:
    """Write a JSON file using UTF-8."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=4, ensure_ascii=False)


def list_real_parquet_files(path: str | Path) -> List[Path]:
    """List real Parquet files, excluding Spark temporary and CRC metadata files."""
    base = Path(path)
    if not base.exists():
        return []

    return sorted(
        file
        for file in base.rglob("*.parquet")
        if file.is_file()
        and "_temporary" not in str(file).lower()
        and not file.name.startswith(".")
    )


def find_latest_parquet_folder(
    base_path: str | Path,
    name_contains: str | None = None,
    name_prefixes: Sequence[str] | None = None,
    excluded_keywords: Sequence[str] | None = None,
) -> Optional[Path]:
    """Find the newest folder containing real Parquet files."""
    base = Path(base_path)
    if not base.exists():
        return None

    excluded = [keyword.lower() for keyword in (excluded_keywords or [])]
    candidates: List[Dict[str, Any]] = []

    for path in base.rglob("*"):
        if not path.is_dir():
            continue

        path_text = str(path).lower()
        name = path.name

        if excluded and any(keyword in path_text for keyword in excluded):
            continue

        if name_contains and name_contains not in name:
            continue

        if name_prefixes and not any(name.startswith(prefix) for prefix in name_prefixes):
            continue

        parquet_files = list_real_parquet_files(path)
        if not parquet_files:
            continue

        candidates.append(
            {
                "path": path,
                "modified_time": max(file.stat().st_mtime for file in parquet_files),
            }
        )

    if not candidates:
        return None

    return max(candidates, key=lambda item: item["modified_time"])["path"]


def sanitize_path_fragment(value: Any) -> str:
    """Make a value safe for use as a folder name fragment."""
    text = str(value).replace(".parquet", "")
    for old in ["/", "\\", ":", " "]:
        text = text.replace(old, "_")
    return text


def union_all_by_name(dataframes: Sequence[DataFrame]) -> DataFrame:
    """Union a non-empty sequence of DataFrames by column name."""
    if not dataframes:
        raise ValueError("At least one DataFrame is required for union.")
    return reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        dataframes,
    )


def add_missing_columns(df: DataFrame, columns: Dict[str, str]) -> DataFrame:
    """Add missing columns with NULL values cast to the requested Spark types."""
    result = df
    for column_name, data_type in columns.items():
        if column_name not in result.columns:
            result = result.withColumn(column_name, F.lit(None).cast(data_type))
    return result


def extract_partition_value(file_path: str | Path, prefix: str) -> Optional[int]:
    """Extract values from path fragments like year=2023 or month=01."""
    for part in Path(file_path).parts:
        if part.lower().startswith(prefix.lower() + "="):
            raw_value = part.split("=", 1)[1]
            try:
                return int(raw_value)
            except ValueError:
                return None
    return None


def file_size_mb(file_path: str | Path, decimals: int = 4) -> float:
    """Return file size in megabytes."""
    return round(Path(file_path).stat().st_size / (1024 * 1024), decimals)


def ensure_directories(paths: Iterable[str | Path]) -> None:
    """Create directories if they do not exist."""
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)
