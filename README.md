# ITMO Big Data Infrastructure — Lab 7 (Data Mart on Scala Spark)

Лабораторная работа №7: интеграция схемы **модель → витрина данных → источник**.

- Источник данных: MongoDB
- Витрина данных: Scala + Spark (`datamart`)
- Модель: PySpark KMeans (`src/kmeans_clustering.py`)

## Что реализовано

1. Разработана витрина данных на Scala (Spark), которая:
   - формирует запросы к MongoDB,
   - выполняет предобработку,
   - подготавливает единый формат данных для модели,
   - загружает результаты модели обратно в MongoDB.
2. Предобработка перенесена на сторону витрины.
3. В модельном контуре ЛР7 используется только готовый датасет из витрины.
4. Добавлен протокол взаимодействия и форматы хранения (`PROTOCOL_LAB7.md`).
5. Добавлен Docker-контур для запуска `модель + витрина + источник`.

## Структура

### Scala витрина
- `datamart/build.sbt`
- `datamart/conf/datamart-local.conf`
- `datamart/conf/datamart-docker.conf`
- `datamart/src/main/scala/itmo/lab7/Main.scala`
- `datamart/src/main/scala/itmo/lab7/service/DataMartService.scala`
- `datamart/src/main/scala/itmo/lab7/mongo/MongoGateway.scala`
- `datamart/src/main/scala/itmo/lab7/model/FeatureRecord.scala`
- `datamart/src/main/scala/itmo/lab7/config/DataMartConfig.scala`

### Python модель и orchestration
- `src/lab7_mart_pipeline.py` — общий запуск ЛР7 пайплайна
- `src/kmeans_clustering.py` — модель KMeans
- `src/download_openfoodfacts_sample.py` — sample для bootstrap источника (при необходимости)

### Конфиги
- `configs/app_config.yaml` — модельные пути/параметры
- `configs/spark_config.yaml` — Spark-конфиг модели
- `configs/lab7_pipeline.yaml` — настройки orchestration
- `configs/mongo_config.yaml` — MongoDB (локально)

## Запуск локально

### 1) Установка зависимостей

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Поднять MongoDB

```bash
docker compose up -d mongodb
```

### 3) Запустить ЛР7 пайплайн

```bash
python3 -m src.lab7_mart_pipeline --bootstrap-if-empty
```

Что делает команда:
- при необходимости подготавливает sample-файл для bootstrap;
- запускает Scala витрину (`prepare`), которая забирает source, делает preprocessing и пишет parquet;
- запускает модель KMeans на parquet;
- запускает Scala витрину (`publish`), которая отправляет результаты модели в MongoDB.

## Запуск в Docker

```bash
docker compose -f docker-compose.lab7.yml up --build model-datamart-pipeline
```

## Дистрибутив

```bash
python3 scripts/make_distribution.py --lab 7
```

Архив: `dist/lab7_distribution.zip`.
