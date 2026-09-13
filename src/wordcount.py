from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

try:
    from src.common import create_spark_session, load_config
except ModuleNotFoundError:
    from common import create_spark_session, load_config


DEFAULT_TEXT = """Spark makes distributed processing easier.
Spark and PySpark are useful for big data processing.
Big data pipelines often start with a WordCount example.
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Spark WordCount example")
    parser.add_argument("--input", default="data/raw/wordcount_input.txt")
    parser.add_argument("--output", default="data/output/wordcount_result")
    return parser.parse_args()


def ensure_input_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(DEFAULT_TEXT, encoding="utf-8")


def main() -> None:
    args = parse_args()
    config = load_config()
    spark = create_spark_session("Lab5WordCount", config["spark"]["master"])

    input_path = Path(args.input)
    ensure_input_file(input_path)

    text_df = spark.read.text(str(input_path))
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

    output_path = Path(args.output)
    if output_path.exists():
        import shutil

        shutil.rmtree(output_path)
    counts_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_path))
    print(f"WordCount output saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
