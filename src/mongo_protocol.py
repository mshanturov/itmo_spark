from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import PyMongoError
import yaml

from src.openfoodfacts_features import FEATURE_COLUMNS


class DataSourceProtocolError(RuntimeError):
    """Raised when interaction with MongoDB data source fails."""


@dataclass(frozen=True)
class MongoConnectionSettings:
    uri: str
    database: str


@dataclass(frozen=True)
class MongoCollectionSettings:
    source: str
    results: str
    runs: str


@dataclass(frozen=True)
class MongoPipelineSettings:
    bootstrap_lines: int
    write_batch_size: int


@dataclass(frozen=True)
class MongoSettings:
    connection: MongoConnectionSettings
    collections: MongoCollectionSettings
    pipeline: MongoPipelineSettings


def _load_yaml(path: str) -> dict:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with config_path.open("r", encoding="utf-8") as file:
        parsed = yaml.safe_load(file)
    if not isinstance(parsed, dict):
        raise ValueError(f"Config file has invalid format: {path}")
    return parsed


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_mongo_settings(config_path: str = "configs/mongo_config.yaml") -> MongoSettings:
    raw = _load_yaml(config_path)

    connection = raw["connection"]
    collections = raw["collections"]
    pipeline = raw["pipeline"]

    return MongoSettings(
        connection=MongoConnectionSettings(
            uri=str(connection["uri"]),
            database=str(connection["database"]),
        ),
        collections=MongoCollectionSettings(
            source=str(collections["source"]),
            results=str(collections["results"]),
            runs=str(collections["runs"]),
        ),
        pipeline=MongoPipelineSettings(
            bootstrap_lines=int(pipeline["bootstrap_lines"]),
            write_batch_size=int(pipeline["write_batch_size"]),
        ),
    )


class MongoProtocolClient:
    def __init__(self, settings: MongoSettings) -> None:
        self._settings = settings

    def _connect(self) -> MongoClient:
        client = MongoClient(self._settings.connection.uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        return client

    def update_run_state(self, run_id: str, state: str, payload: dict[str, object] | None = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        document = {
            "state": state,
            "updated_at": now,
        }
        if payload:
            document.update(payload)

        try:
            with self._connect() as client:
                db = client[self._settings.connection.database]
                db[self._settings.collections.runs].update_one(
                    {"run_id": run_id},
                    {
                        "$setOnInsert": {"run_id": run_id, "created_at": now},
                        "$set": document,
                    },
                    upsert=True,
                )
        except PyMongoError as error:
            raise DataSourceProtocolError(f"Failed to update run state in MongoDB: {error}") from error

    def source_is_empty(self) -> bool:
        try:
            with self._connect() as client:
                db = client[self._settings.connection.database]
                count = db[self._settings.collections.source].count_documents({}, limit=1)
                return count == 0
        except PyMongoError as error:
            raise DataSourceProtocolError(f"Failed to check source collection state: {error}") from error

    def bootstrap_source_from_jsonl(self, jsonl_path: Path, max_lines: int) -> int:
        if not jsonl_path.exists():
            raise FileNotFoundError(f"Bootstrap file not found: {jsonl_path}")

        documents: list[dict[str, object]] = []

        with jsonl_path.open("r", encoding="utf-8") as file:
            for index, line in enumerate(file):
                if index >= max_lines:
                    break

                record = json.loads(line)
                nutriments = record.get("nutriments")
                if not isinstance(nutriments, dict):
                    continue

                extracted_nutriments = {
                    "energy-kcal_100g": _to_float(nutriments.get("energy-kcal_100g")),
                    "fat_100g": _to_float(nutriments.get("fat_100g")),
                    "carbohydrates_100g": _to_float(nutriments.get("carbohydrates_100g")),
                    "sugars_100g": _to_float(nutriments.get("sugars_100g")),
                    "proteins_100g": _to_float(nutriments.get("proteins_100g")),
                    "salt_100g": _to_float(nutriments.get("salt_100g")),
                }

                if any(value is None for value in extracted_nutriments.values()):
                    continue

                documents.append(
                    {
                        "code": str(record.get("code", "")),
                        "product_name": str(record.get("product_name", "")),
                        "nutriments": extracted_nutriments,
                        "source": "openfoodfacts",
                    }
                )

        if not documents:
            return 0

        try:
            with self._connect() as client:
                db = client[self._settings.connection.database]
                db[self._settings.collections.source].insert_many(documents, ordered=False)
            return len(documents)
        except PyMongoError as error:
            raise DataSourceProtocolError(f"Failed to bootstrap source data in MongoDB: {error}") from error

    def export_source_to_jsonl(self, output_path: Path) -> int:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        exported = 0

        try:
            with self._connect() as client:
                db = client[self._settings.connection.database]
                cursor = db[self._settings.collections.source].find(
                    {},
                    {
                        "_id": 0,
                        "code": 1,
                        "product_name": 1,
                        "nutriments": 1,
                    },
                )

                with output_path.open("w", encoding="utf-8") as file:
                    for document in cursor:
                        file.write(json.dumps(document, ensure_ascii=False) + "\n")
                        exported += 1

            return exported
        except PyMongoError as error:
            raise DataSourceProtocolError(f"Failed to export source data from MongoDB: {error}") from error

    def load_predictions(
        self,
        run_id: str,
        metrics: dict[str, object],
        rows: list[dict[str, object]],
    ) -> int:
        if not rows:
            return 0

        now = datetime.now(timezone.utc).isoformat()
        best_k = int(metrics["best_k"])
        best_silhouette = float(metrics["best_silhouette"])

        documents: list[dict[str, object]] = []
        for row in rows:
            feature_values = {feature_name: float(row[feature_name]) for feature_name in FEATURE_COLUMNS}
            documents.append(
                {
                    "run_id": run_id,
                    "processed_at": now,
                    "model": "kmeans",
                    "best_k": best_k,
                    "best_silhouette": best_silhouette,
                    "code": str(row.get("code", "")),
                    "product_name": str(row.get("product_name", "")),
                    "features": feature_values,
                    "cluster_id": int(row["prediction"]),
                }
            )

        batch_size = max(1, self._settings.pipeline.write_batch_size)

        try:
            with self._connect() as client:
                db = client[self._settings.connection.database]
                collection = db[self._settings.collections.results]

                for offset in range(0, len(documents), batch_size):
                    batch = documents[offset : offset + batch_size]
                    collection.insert_many(batch, ordered=False)

            return len(documents)
        except PyMongoError as error:
            raise DataSourceProtocolError(f"Failed to load predictions into MongoDB: {error}") from error
