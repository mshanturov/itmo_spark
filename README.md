# ITMO Big Data Infrastructure — Lab 5 (PySpark KMeans)

Лабораторная работа №5: Spark-приложение на PySpark,
проверка Spark компонентов (WordCount) и кластеризация OpenFoodFacts с помощью KMeans.

## Что улучшено по замечаниям

- Spark-конфиг вынесен в отдельный файл: `configs/spark_config.yaml`.
- Бизнес-конфиг вынесен отдельно: `configs/app_config.yaml`.
- Исходники декомпозированы по файлам и оформлены в OOP-стиле (job-классы).
- Нет широких `except Exception` в коде.
- Добавлен шаблон конфига для ЛР8: `configs/spark_config_lab8_template.yaml`.

## Структура проекта

- `src/settings.py` — dataclass-настройки и загрузка конфигов.
- `src/spark_session_factory.py` — фабрика SparkSession.
- `src/openfoodfacts_features.py` — схема, признаки и их обоснование.
- `src/wordcount.py` — класс `WordCountJob`.
- `src/download_openfoodfacts_sample.py` — класс `OpenFoodFactsSampler`.
- `src/preprocess_openfoodfacts.py` — класс `OpenFoodFactsPreprocessor`.
- `src/kmeans_clustering.py` — класс `KMeansClusteringJob`.
- `src/run_pipeline.py` — класс `Lab5Pipeline` для последовательного запуска.

## Конфигурация

### `configs/app_config.yaml`
Содержит пути к данным, параметры сэмплирования и KMeans.

### `configs/spark_config.yaml`
Содержит `master` и Spark-параметры:
- `spark.sql.shuffle.partitions`
- `spark.sql.adaptive.enabled`
- `spark.driver.memory`
- `spark.executor.memory`
- `spark.serializer`
- `spark.ui.showConsoleProgress`

## Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Для Windows можно использовать `py -3.11` вместо `python3`.

## Запуск

### Проверка Spark (WordCount)

```bash
python3 -m src.wordcount
```

Результат: `data/output/wordcount_result`.

### Кластеризация (пошагово)

```bash
python3 -m src.download_openfoodfacts_sample
python3 -m src.preprocess_openfoodfacts
python3 -m src.kmeans_clustering
```

### Полный пайплайн

```bash
python3 -m src.run_pipeline
```

## Почему выбраны эти фичи

Использованы 6 числовых признаков на 100 г (`energy_kcal_100g`, `fat_100g`,
`carbohydrates_100g`, `sugars_100g`, `proteins_100g`, `salt_100g`), потому что:
- они доступны у большого числа продуктов;
- это базовые пищевые характеристики, хорошо разделяющие категории продуктов;
- признаки числовые и подходят для KMeans после масштабирования.

## Артефакты

- `data/processed/openfoodfacts_features.parquet`
- `data/output/openfoodfacts_clusters.parquet`
- `data/output/cluster_metrics.json`
- `data/output/cluster_centers.json`

## Дистрибутив

```bash
python3 scripts/make_distribution.py
```

Архив: `dist/lab5_distribution.zip`.
