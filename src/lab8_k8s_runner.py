from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
from pathlib import Path

from src.download_openfoodfacts_sample import OpenFoodFactsSampler
from src.kmeans_clustering import KMeansClusteringJob
from src.lab6_mongo_pipeline import MongoBackedLab6Pipeline
from src.lab7_mart_pipeline import Lab7Pipeline, load_pipeline_settings
from src.preprocess_openfoodfacts import OpenFoodFactsPreprocessor
from src.settings import AppSettings, load_settings
from src.spark_session_factory import SparkSessionFactory


@dataclass(frozen=True)
class RunnerConfig:
    app_config: str
    spark_config: str
    mongo_config: str
    lab7_pipeline_config: str
    datamart_config: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Lab8 stage workflow inside Kubernetes")
    parser.add_argument("--stage", choices=["lab5", "lab6", "lab7"], required=True)
    parser.add_argument("--app-config", default="configs/app_config.yaml")
    parser.add_argument("--spark-config", default="configs/spark_config_k8s.yaml")
    parser.add_argument("--mongo-config", default="configs/mongo_config_k8s.yaml")
    parser.add_argument("--lab7-pipeline-config", default="configs/lab7_pipeline_k8s.yaml")
    parser.add_argument("--datamart-config", default="/app/datamart/conf/datamart-k8s.conf")
    parser.add_argument("--bootstrap-if-empty", action="store_true")
    return parser.parse_args()


def run_lab5_stage(settings: AppSettings) -> None:
    spark_factory = SparkSessionFactory(settings.spark)

    sampler = OpenFoodFactsSampler(settings)
    sampler.run(max_lines_override=settings.sampling.max_lines)

    preprocessor = OpenFoodFactsPreprocessor(settings, spark_factory)
    preprocessor.run()

    clustering_job = KMeansClusteringJob(settings, spark_factory)
    clustering_job.run()


def run_lab6_stage(config: RunnerConfig, bootstrap_if_empty: bool) -> None:
    pipeline = MongoBackedLab6Pipeline(
        app_config_path=config.app_config,
        spark_config_path=config.spark_config,
        mongo_config_path=config.mongo_config,
    )
    pipeline.run(bootstrap_if_empty=bootstrap_if_empty)


def run_lab7_stage(config: RunnerConfig, bootstrap_if_empty: bool) -> None:
    os.environ.setdefault("SBT_BIN", "/opt/sbt/bin/sbt")
    pipeline_settings = load_pipeline_settings(config.lab7_pipeline_config)
    pipeline = Lab7Pipeline(pipeline_settings)
    pipeline.run(bootstrap_if_empty=bootstrap_if_empty, datamart_config_path=config.datamart_config)


def main() -> None:
    args = parse_args()
    config = RunnerConfig(
        app_config=args.app_config,
        spark_config=args.spark_config,
        mongo_config=args.mongo_config,
        lab7_pipeline_config=args.lab7_pipeline_config,
        datamart_config=args.datamart_config,
    )

    settings = load_settings(config.app_config, config.spark_config)

    data_root = Path(settings.data.raw_sample_jsonl).resolve().parent
    data_root.mkdir(parents=True, exist_ok=True)

    if args.stage == "lab5":
        run_lab5_stage(settings)
    elif args.stage == "lab6":
        run_lab6_stage(config, bootstrap_if_empty=args.bootstrap_if_empty)
    else:
        run_lab7_stage(config, bootstrap_if_empty=args.bootstrap_if_empty)


if __name__ == "__main__":
    main()
