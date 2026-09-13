from __future__ import annotations

import argparse

from pymongo import MongoClient

from src.mongo_protocol import load_mongo_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check MongoDB collections used by Lab6 pipeline")
    parser.add_argument("--mongo-config", default="configs/mongo_config.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_mongo_settings(args.mongo_config)

    with MongoClient(settings.connection.uri, serverSelectionTimeoutMS=5000) as client:
        db = client[settings.connection.database]

        source_count = db[settings.collections.source].count_documents({})
        results_count = db[settings.collections.results].count_documents({})
        runs_count = db[settings.collections.runs].count_documents({})

        print(f"source_count={source_count}")
        print(f"results_count={results_count}")
        print(f"runs_count={runs_count}")

        latest_run = db[settings.collections.runs].find_one(sort=[("updated_at", -1)])
        if latest_run:
            latest_run.pop("_id", None)
            print(f"latest_run={latest_run}")


if __name__ == "__main__":
    main()
