from __future__ import annotations

import argparse
import shutil
from pathlib import Path
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.openfoodfacts_features import FEATURE_BOUNDS, FEATURE_COLUMNS, OPENFOODFACTS_SCHEMA
from src.settings import AppSettings, load_settings
from src.spark_session_factory import SparkSessionFactory


class OpenFoodFactsPreprocessor:
    def __init__(self, settings: AppSettings, spark_factory: SparkSessionFactory) -> None:
        self._settings = settings
        self._spark_factory = spark_factory

    def _select_features(self, dataframe: DataFrame) -> DataFrame:
        features_df = dataframe.select(
            F.col("product_name").alias("product_name"),
            F.col("code").alias("code"),
            F.col("nutriments").getField("energy-kcal_100g").cast("double").alias("energy_kcal_100g"),
            F.col("nutriments").getField("fat_100g").cast("double").alias("fat_100g"),
            F.col("nutriments").getField("carbohydrates_100g").cast("double").alias("carbohydrates_100g"),
            F.col("nutriments").getField("sugars_100g").cast("double").alias("sugars_100g"),
            F.col("nutriments").getField("proteins_100g").cast("double").alias("proteins_100g"),
            F.col("nutriments").getField("salt_100g").cast("double").alias("salt_100g"),
        )

        filtered_df = features_df.dropna(subset=FEATURE_COLUMNS)
        for feature_name, (lower_bound, upper_bound) in FEATURE_BOUNDS.items():
            filtered_df = filtered_df.filter(F.col(feature_name).between(lower_bound, upper_bound))

        return filtered_df

    def run(self) -> None:
        spark = self._spark_factory.create("Lab5Preprocess")

        input_path = Path(self._settings.data.raw_sample_jsonl)
        output_path = Path(self._settings.data.processed_parquet)

        if not input_path.exists():
            raise FileNotFoundError(
                f"Raw sample not found at {input_path}. Run src/download_openfoodfacts_sample.py first."
            )

        raw_df = spark.read.text(str(input_path))
        parsed_df = raw_df.select(F.from_json(F.col("value"), OPENFOODFACTS_SCHEMA).alias("item")).select("item.*")
        processed_df = self._select_features(parsed_df)

        if output_path.exists():
            shutil.rmtree(output_path)

        processed_df.write.mode("overwrite").parquet(str(output_path))

        raw_count = parsed_df.count()
        processed_count = processed_df.count()
        print(f"Raw rows: {raw_count}")
        print(f"Processed rows: {processed_count}")
        print(f"Saved processed dataset to: {output_path}")

        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preprocess OpenFoodFacts sample for clustering")
    parser.add_argument("--app-config", default="configs/app_config.yaml")
    parser.add_argument("--spark-config", default="configs/spark_config.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings(args.app_config, args.spark_config)
    spark_factory = SparkSessionFactory(settings.spark)
    preprocessor = OpenFoodFactsPreprocessor(settings, spark_factory)
    preprocessor.run()


if __name__ == "__main__":
    main()
