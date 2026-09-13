from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import subprocess

import yaml

from src.download_openfoodfacts_sample import OpenFoodFactsSampler
from src.kmeans_clustering import KMeansClusteringJob
from src.settings import load_settings
from src.spark_session_factory import SparkSessionFactory


@dataclass(frozen=True)
class DataMartPipelineSettings:
    project_dir: str
    main_class: str
    local_config: str
    docker_config: str
    bootstrap_enabled_by_default: bool
    bootstrap_source_file: str
    bootstrap_source_lines: int
    app_config: str
    spark_config: str


def load_pipeline_settings(path: str = "configs/lab7_pipeline.yaml") -> DataMartPipelineSettings:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Pipeline config not found: {path}")

    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    datamart_config = config["datamart"]
    model_config = config["model"]
    bootstrap_config = config["bootstrap"]

    return DataMartPipelineSettings(
        project_dir=str(datamart_config["project_dir"]),
        main_class=str(datamart_config["main_class"]),
        local_config=str(datamart_config["local_config"]),
        docker_config=str(datamart_config["docker_config"]),
        bootstrap_enabled_by_default=bool(bootstrap_config["enabled_by_default"]),
        bootstrap_source_file=str(bootstrap_config["source_file"]),
        bootstrap_source_lines=int(bootstrap_config["source_lines"]),
        app_config=str(model_config["app_config"]),
        spark_config=str(model_config["spark_config"]),
    )


class Lab7Pipeline:
    def __init__(self, settings: DataMartPipelineSettings) -> None:
        self._settings = settings
        model_settings = load_settings(settings.app_config, settings.spark_config)
        spark_factory = SparkSessionFactory(model_settings.spark)
        self._sampler = OpenFoodFactsSampler(model_settings)
        self._kmeans_job = KMeansClusteringJob(model_settings, spark_factory)

    def _run_id(self) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"lab7-run-{timestamp}"

    def _run_datamart_command(self, args: list[str]) -> None:
        subprocess.run(
            ["sbt", "-batch", f"runMain {self._settings.main_class} {' '.join(args)}"],
            cwd=self._settings.project_dir,
            check=True,
        )

    def run(self, bootstrap_if_empty: bool, datamart_config_path: str) -> None:
        run_id = self._run_id()

        if bootstrap_if_empty:
            self._sampler.run(max_lines_override=self._settings.bootstrap_source_lines)

        prepare_args = [
            "prepare",
            "--config",
            datamart_config_path,
            "--run-id",
            run_id,
        ]
        if bootstrap_if_empty:
            prepare_args.extend(
                [
                    "--bootstrap-if-empty",
                    "--bootstrap-file",
                    f"../{self._settings.bootstrap_source_file}",
                    "--bootstrap-lines",
                    str(self._settings.bootstrap_source_lines),
                ]
            )

        self._run_datamart_command(prepare_args)

        self._kmeans_job.run()

        publish_args = [
            "publish",
            "--config",
            datamart_config_path,
            "--run-id",
            run_id,
        ]
        self._run_datamart_command(publish_args)

        print(f"Lab7 pipeline completed. run_id={run_id}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lab7 pipeline: model -> data mart -> source")
    parser.add_argument("--pipeline-config", default="configs/lab7_pipeline.yaml")
    parser.add_argument("--bootstrap-if-empty", action="store_true")
    parser.add_argument("--datamart-config", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_pipeline_settings(args.pipeline_config)
    bootstrap = args.bootstrap_if_empty or settings.bootstrap_enabled_by_default
    datamart_config_path = args.datamart_config if args.datamart_config else settings.local_config
    pipeline = Lab7Pipeline(settings)
    pipeline.run(bootstrap_if_empty=bootstrap, datamart_config_path=datamart_config_path)


if __name__ == "__main__":
    main()
