from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

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

OPENFOODFACTS_SCHEMA = T.StructType(
    [
        T.StructField("code", T.StringType(), True),
        T.StructField("product_name", T.StringType(), True),
        T.StructField(
            "nutriments",
            T.StructType(
                [
                    T.StructField("energy-kcal_100g", T.DoubleType(), True),
                    T.StructField("fat_100g", T.DoubleType(), True),
                    T.StructField("carbohydrates_100g", T.DoubleType(), True),
                    T.StructField("sugars_100g", T.DoubleType(), True),
                    T.StructField("proteins_100g", T.DoubleType(), True),
                    T.StructField("salt_100g", T.DoubleType(), True),
                ]
            ),
            True,
        ),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preprocess OpenFoodFacts sample for clustering")
    parser.add_argument("--config", default="config.yaml")
    return parser.parse_args()


def select_features(dataframe: DataFrame) -> DataFrame:
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

    filtered_df = (
        features_df.dropna(subset=FEATURE_COLUMNS)
        .filter(F.col("energy_kcal_100g").between(0, 1200))
        .filter(F.col("fat_100g").between(0, 100))
        .filter(F.col("carbohydrates_100g").between(0, 100))
        .filter(F.col("sugars_100g").between(0, 100))
        .filter(F.col("proteins_100g").between(0, 100))
        .filter(F.col("salt_100g").between(0, 100))
    )

    return filtered_df


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    spark = create_spark_session("Lab5Preprocess", config["spark"]["master"])

    input_path = Path(config["data"]["raw_sample_jsonl"])
    output_path = Path(config["data"]["processed_parquet"])

    if not input_path.exists():
        raise FileNotFoundError(
            f"Raw sample not found at {input_path}. Run src/download_openfoodfacts_sample.py first."
        )

    raw_df = spark.read.text(str(input_path))
    parsed_df = raw_df.select(F.from_json(F.col("value"), OPENFOODFACTS_SCHEMA).alias("item")).select("item.*")
    processed_df = select_features(parsed_df)

    if output_path.exists():
        import shutil

        shutil.rmtree(output_path)

    processed_df.write.mode("overwrite").parquet(str(output_path))

    raw_count = parsed_df.count()
    processed_count = processed_df.count()
    print(f"Raw rows: {raw_count}")
    print(f"Processed rows: {processed_count}")
    print(f"Saved processed dataset to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
