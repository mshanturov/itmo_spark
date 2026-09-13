from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class DataSettings:
    source_url: str
    raw_sample_jsonl: str
    processed_parquet: str
    predictions_parquet: str
    metrics_json: str
    centers_json: str


@dataclass(frozen=True)
class SamplingSettings:
    max_lines: int


@dataclass(frozen=True)
class ClusteringSettings:
    k_values: list[int]
    seed: int
    max_iter: int


@dataclass(frozen=True)
class SparkSettings:
    master: str
    default_configs: dict[str, str]


@dataclass(frozen=True)
class AppSettings:
    data: DataSettings
    sampling: SamplingSettings
    clustering: ClusteringSettings
    spark: SparkSettings


def _load_yaml(path: str) -> dict:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with config_path.open("r", encoding="utf-8") as file:
        parsed = yaml.safe_load(file)
    if not isinstance(parsed, dict):
        raise ValueError(f"Config file has invalid format: {path}")
    return parsed


def load_settings(
    app_config_path: str = "configs/app_config.yaml",
    spark_config_path: str = "configs/spark_config.yaml",
) -> AppSettings:
    app_config = _load_yaml(app_config_path)
    spark_config = _load_yaml(spark_config_path)

    data = app_config["data"]
    sampling = app_config["sampling"]
    clustering = app_config["clustering"]

    k_values = [int(value) for value in clustering["k_values"]]
    if not k_values:
        raise ValueError("clustering.k_values must contain at least one value")

    default_configs = {str(key): str(value) for key, value in spark_config["default_configs"].items()}

    return AppSettings(
        data=DataSettings(
            source_url=str(data["source_url"]),
            raw_sample_jsonl=str(data["raw_sample_jsonl"]),
            processed_parquet=str(data["processed_parquet"]),
            predictions_parquet=str(data["predictions_parquet"]),
            metrics_json=str(data["metrics_json"]),
            centers_json=str(data["centers_json"]),
        ),
        sampling=SamplingSettings(max_lines=int(sampling["max_lines"])),
        clustering=ClusteringSettings(
            k_values=k_values,
            seed=int(clustering["seed"]),
            max_iter=int(clustering["max_iter"]),
        ),
        spark=SparkSettings(
            master=str(spark_config["master"]),
            default_configs=default_configs,
        ),
    )
