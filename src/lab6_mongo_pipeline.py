from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from src.download_openfoodfacts_sample import OpenFoodFactsSampler
from src.kmeans_clustering import KMeansClusteringJob
from src.mongo_protocol import DataSourceProtocolError, MongoProtocolClient, load_mongo_settings
from src.preprocess_openfoodfacts import OpenFoodFactsPreprocessor
from src.settings import load_settings
from src.spark_session_factory import SparkSessionFactory


class MongoBackedLab6Pipeline:
    def __init__(
        self,
        app_config_path: str,
        spark_config_path: str,
        mongo_config_path: str,
    ) -> None:
        self._settings = load_settings(app_config_path, spark_config_path)
        self._mongo_settings = load_mongo_settings(mongo_config_path)
        self._mongo_client = MongoProtocolClient(self._mongo_settings)
        self._spark_factory = SparkSessionFactory(self._settings.spark)

    def _generate_run_id(self) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"lab6-run-{timestamp}"

    def _bootstrap_source_if_needed(self) -> int:
        if not self._mongo_client.source_is_empty():
            return 0

        sampler = OpenFoodFactsSampler(self._settings)
        sampler.run(max_lines_override=self._mongo_settings.pipeline.bootstrap_lines)

        inserted = self._mongo_client.bootstrap_source_from_jsonl(
            Path(self._settings.data.raw_sample_jsonl),
            self._mongo_settings.pipeline.bootstrap_lines,
        )
        return inserted

    def _read_predictions(self) -> list[dict[str, object]]:
        spark = self._spark_factory.create("Lab6ReadPredictions")
        predictions_df = spark.read.parquet(self._settings.data.predictions_parquet)
        rows = [row.asDict(recursive=True) for row in predictions_df.toLocalIterator()]
        spark.stop()
        return rows

    def run(self, bootstrap_if_empty: bool) -> None:
        run_id = self._generate_run_id()
        self._mongo_client.update_run_state(run_id, "started")

        if bootstrap_if_empty:
            inserted = self._bootstrap_source_if_needed()
            if inserted > 0:
                self._mongo_client.update_run_state(run_id, "bootstrapped", {"bootstrapped_rows": inserted})

        exported = self._mongo_client.export_source_to_jsonl(Path(self._settings.data.raw_sample_jsonl))
        if exported == 0:
            raise RuntimeError("Source collection is empty. Nothing to export for model run.")
        self._mongo_client.update_run_state(run_id, "exported", {"exported_rows": exported})

        preprocessor = OpenFoodFactsPreprocessor(self._settings, self._spark_factory)
        preprocessor.run()

        clustering_job = KMeansClusteringJob(self._settings, self._spark_factory)
        clustering_job.run()

        metrics_path = Path(self._settings.data.metrics_json)
        if not metrics_path.exists():
            raise FileNotFoundError(f"Metrics file not found: {metrics_path}")
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

        prediction_rows = self._read_predictions()
        loaded = self._mongo_client.load_predictions(run_id, metrics, prediction_rows)

        self._mongo_client.update_run_state(
            run_id,
            "completed",
            {
                "loaded_rows": loaded,
                "best_k": int(metrics["best_k"]),
                "best_silhouette": float(metrics["best_silhouette"]),
            },
        )

        print(f"Run id: {run_id}")
        print(f"Exported rows from MongoDB: {exported}")
        print(f"Loaded prediction rows into MongoDB: {loaded}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lab 6 pipeline: PySpark + MongoDB source")
    parser.add_argument("--app-config", default="configs/app_config.yaml")
    parser.add_argument("--spark-config", default="configs/spark_config.yaml")
    parser.add_argument("--mongo-config", default="configs/mongo_config.yaml")
    parser.add_argument("--bootstrap-if-empty", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pipeline = MongoBackedLab6Pipeline(
        app_config_path=args.app_config,
        spark_config_path=args.spark_config,
        mongo_config_path=args.mongo_config,
    )

    try:
        pipeline.run(bootstrap_if_empty=args.bootstrap_if_empty)
    except DataSourceProtocolError as error:
        raise RuntimeError(f"MongoDB protocol failure: {error}") from error


if __name__ == "__main__":
    main()
