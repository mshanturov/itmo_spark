from __future__ import annotations

from pyspark.sql import SparkSession

from src.settings import SparkSettings


class SparkSessionFactory:
    def __init__(self, settings: SparkSettings) -> None:
        self._settings = settings

    def create(self, app_name: str) -> SparkSession:
        builder = SparkSession.builder.appName(app_name).master(self._settings.master)
        for key, value in self._settings.default_configs.items():
            builder = builder.config(key, value)
        return builder.getOrCreate()
