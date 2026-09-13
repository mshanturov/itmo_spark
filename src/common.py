from __future__ import annotations

from pathlib import Path

import yaml
from pyspark.sql import SparkSession


def load_config(config_path: str = "config.yaml") -> dict:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def create_spark_session(app_name: str, master: str = "local[*]") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
