# ITMO Big Data Infrastructure — Lab 6 (PySpark + MongoDB)

Лабораторная работа №6: реализация схемы **модель ↔ источник данных**,
где источником выступает **MongoDB**, а модель реализована на **PySpark (KMeans)**.

## Что реализовано

- Выгрузка исходных данных из MongoDB при каждом запуске модели.
- Запуск PySpark-пайплайна предобработки и кластеризации.
- Загрузка результатов модели в MongoDB сразу после завершения.
- Протокол взаимодействия и форматы хранения данных.
- Docker-конфигурация для запуска `MongoDB + model pipeline`.

## Структура

- `src/mongo_protocol.py` — протокол взаимодействия с MongoDB.
- `src/lab6_mongo_pipeline.py` — оркестратор ЛР6.
- `src/check_mongo_results.py` — проверка наполнения коллекций.
- `src/preprocess_openfoodfacts.py` — предобработка.
- `src/kmeans_clustering.py` — KMeans.
- `src/download_openfoodfacts_sample.py` — загрузка sample OpenFoodFacts.
- `configs/mongo_config.yaml` — Mongo-конфиг для локального запуска.
- `configs/mongo_config.docker.yaml` — Mongo-конфиг для Docker.
- `configs/spark_config.yaml` — Spark-конфиг в отдельном файле.
- `PROTOCOL_LAB6.md` — формальный протокол взаимодействия.

## Конфигурация

### `configs/app_config.yaml`
Пути к данным, параметры sampling и KMeans.

### `configs/spark_config.yaml`
Spark master и Spark-параметры (shuffle/adaptive/memory/serializer).

### `configs/mongo_config.yaml`
- URI подключения к MongoDB
- Имя БД
- Имена коллекций (`source`, `results`, `runs`)
- Параметры пайплайна (`bootstrap_lines`, `write_batch_size`)

## Локальный запуск

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

1) Поднять MongoDB в Docker:
```bash
docker compose up -d mongodb
```

2) Запустить пайплайн ЛР6:
```bash
python3 -m src.lab6_mongo_pipeline --bootstrap-if-empty
```

3) Проверить, что данные и результаты записаны в MongoDB:
```bash
python3 -m src.check_mongo_results
```

## Запуск полностью в Docker

```bash
docker compose up --build model-pipeline
```

## Дистрибутив

```bash
python3 scripts/make_distribution.py --lab 6
```

Архив: `dist/lab6_distribution.zip`.
