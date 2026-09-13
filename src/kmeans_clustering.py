from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
import sys

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.openfoodfacts_features import FEATURE_COLUMNS
from src.settings import AppSettings, load_settings
from src.spark_session_factory import SparkSessionFactory


class KMeansClusteringJob:
    def __init__(self, settings: AppSettings, spark_factory: SparkSessionFactory) -> None:
        self._settings = settings
        self._spark_factory = spark_factory

    def _build_scaled_dataset(self, dataset: DataFrame) -> DataFrame:
        assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features")
        featured = assembler.transform(dataset)

        scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
        scaler_model = scaler.fit(featured)
        return scaler_model.transform(featured)

    def _select_best_model(self, scaled_df: DataFrame) -> tuple[KMeansModel, dict[str, float], int, float]:
        evaluator = ClusteringEvaluator(
            featuresCol="scaled_features",
            predictionCol="prediction",
            metricName="silhouette",
            distanceMeasure="squaredEuclidean",
        )

        metrics: dict[str, float] = {}
        best_model: KMeansModel | None = None
        best_score = float("-inf")
        best_k = -1

        for k in self._settings.clustering.k_values:
            model = KMeans(
                k=k,
                seed=self._settings.clustering.seed,
                maxIter=self._settings.clustering.max_iter,
                featuresCol="scaled_features",
                predictionCol="prediction",
            ).fit(scaled_df)
            predictions = model.transform(scaled_df)
            score = float(evaluator.evaluate(predictions))
            metrics[str(k)] = score

            if score > best_score:
                best_score = score
                best_model = model
                best_k = k

        if best_model is None:
            raise RuntimeError("Failed to train KMeans model")

        return best_model, metrics, best_k, best_score

    def run(self) -> None:
        spark = self._spark_factory.create("Lab5KMeans")

        input_path = Path(self._settings.data.processed_parquet)
        predictions_path = Path(self._settings.data.predictions_parquet)
        metrics_path = Path(self._settings.data.metrics_json)
        centers_path = Path(self._settings.data.centers_json)

        if not input_path.exists():
            raise FileNotFoundError(
                f"Processed dataset not found at {input_path}. "
                "For Lab7 run the data mart prepare step first."
            )

        dataset = spark.read.parquet(str(input_path))
        scaled_df = self._build_scaled_dataset(dataset)
        best_model, metrics, best_k, best_score = self._select_best_model(scaled_df)

        final_predictions = best_model.transform(scaled_df).select(
            "code", "product_name", *FEATURE_COLUMNS, "prediction"
        )

        if predictions_path.exists():
            shutil.rmtree(predictions_path)

        final_predictions.write.mode("overwrite").parquet(str(predictions_path))

        centers = [list(map(float, center)) for center in best_model.clusterCenters()]

        metrics_payload = {
            "tested_k_values": self._settings.clustering.k_values,
            "silhouette_by_k": metrics,
            "best_k": int(best_k),
            "best_silhouette": float(best_score),
            "rows_used": int(final_predictions.count()),
        }

        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        metrics_path.write_text(json.dumps(metrics_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        centers_path.write_text(
            json.dumps({"best_k": int(best_k), "cluster_centers": centers}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        print(f"Best K: {best_k}, silhouette: {best_score:.4f}")
        print(f"Predictions saved to: {predictions_path}")
        print(f"Metrics saved to: {metrics_path}")
        print(f"Centers saved to: {centers_path}")

        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train PySpark KMeans clustering on OpenFoodFacts")
    parser.add_argument("--app-config", default="configs/app_config.yaml")
    parser.add_argument("--spark-config", default="configs/spark_config.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings(args.app_config, args.spark_config)
    spark_factory = SparkSessionFactory(settings.spark)
    job = KMeansClusteringJob(settings, spark_factory)
    job.run()


if __name__ == "__main__":
    main()
