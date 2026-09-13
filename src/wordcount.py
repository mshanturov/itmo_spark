from __future__ import annotations

import argparse
import shutil
from pathlib import Path
import sys

from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.settings import load_settings
from src.spark_session_factory import SparkSessionFactory

DEFAULT_TEXT = """Spark makes distributed processing easier.
Spark and PySpark are useful for big data processing.
Big data pipelines often start with a WordCount example.
"""


class WordCountJob:
    def __init__(self, input_path: Path, output_path: Path, spark_factory: SparkSessionFactory) -> None:
        self._input_path = input_path
        self._output_path = output_path
        self._spark_factory = spark_factory

    def _ensure_input_file(self) -> None:
        self._input_path.parent.mkdir(parents=True, exist_ok=True)
        if not self._input_path.exists():
            self._input_path.write_text(DEFAULT_TEXT, encoding="utf-8")

    def run(self) -> None:
        self._ensure_input_file()
        spark = self._spark_factory.create("Lab5WordCount")

        text_df = spark.read.text(str(self._input_path))
        words_df = (
            text_df.select(
                F.explode(
                    F.split(
                        F.regexp_replace(F.lower(F.col("value")), r"[^a-zа-я0-9\s]", ""),
                        r"\s+",
                    )
                ).alias("word")
            )
            .filter(F.col("word") != "")
        )

        counts_df = words_df.groupBy("word").count().orderBy(F.desc("count"), F.asc("word"))
        counts_df.show(20, truncate=False)

        if self._output_path.exists():
            shutil.rmtree(self._output_path)

        counts_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(self._output_path))
        print(f"WordCount output saved to: {self._output_path}")

        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Spark WordCount example")
    parser.add_argument("--app-config", default="configs/app_config.yaml")
    parser.add_argument("--spark-config", default="configs/spark_config.yaml")
    parser.add_argument("--input", default="data/raw/wordcount_input.txt")
    parser.add_argument("--output", default="data/output/wordcount_result")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings(args.app_config, args.spark_config)
    spark_factory = SparkSessionFactory(settings.spark)
    job = WordCountJob(Path(args.input), Path(args.output), spark_factory)
    job.run()


if __name__ == "__main__":
    main()
