from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler

try:
    from src.common import create_spark_session, load_config
except ModuleNotFoundError:
    from common import create_spark_session, load_config


FEATURE_COLUMNS = [
    "energy_kcal_100g",
    "fat_100g",
    "carbohydrates_100g",
    "sugars_100g",
    "proteins_100g",
    "salt_100g",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train PySpark KMeans clustering on OpenFoodFacts")
    parser.add_argument("--config", default="config.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    spark = create_spark_session("Lab5KMeans", config["spark"]["master"])

    input_path = Path(config["data"]["processed_parquet"])
    predictions_path = Path(config["data"]["predictions_parquet"])
    metrics_path = Path(config["data"]["metrics_json"])
    centers_path = Path(config["data"]["centers_json"])

    if not input_path.exists():
        raise FileNotFoundError(
            f"Processed dataset not found at {input_path}. Run src/preprocess_openfoodfacts.py first."
        )

    dataset = spark.read.parquet(str(input_path))

    assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features")
    featured = assembler.transform(dataset)

    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    scaler_model = scaler.fit(featured)
    scaled_df = scaler_model.transform(featured)

    evaluator = ClusteringEvaluator(
        featuresCol="scaled_features",
        predictionCol="prediction",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    )

    seed = int(config["clustering"]["seed"])
    max_iter = int(config["clustering"]["max_iter"])
    k_values = [int(k) for k in config["clustering"]["k_values"]]

    metrics: dict[str, float] = {}
    best_model = None
    best_score = float("-inf")
    best_k = None

    for k in k_values:
        model = KMeans(
            k=k,
            seed=seed,
            maxIter=max_iter,
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

    if best_model is None or best_k is None:
        raise RuntimeError("Failed to train KMeans model")

    final_predictions = best_model.transform(scaled_df).select(
        "code", "product_name", *FEATURE_COLUMNS, "prediction"
    )

    if predictions_path.exists():
        import shutil

        shutil.rmtree(predictions_path)

    final_predictions.write.mode("overwrite").parquet(str(predictions_path))

    centers = [list(map(float, center)) for center in best_model.clusterCenters()]

    metrics_payload = {
        "tested_k_values": k_values,
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


if __name__ == "__main__":
    main()
