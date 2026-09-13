# ITMO Big Data Infrastructure — Lab 5 (PySpark KMeans)

Лабораторная работа №5: разработка Spark-приложения на PySpark,
проверка Spark компонентов (WordCount) и построение модели кластеризации (KMeans)
на данных OpenFoodFacts.

## Структура

- `src/wordcount.py` — проверка работоспособности Spark (WordCount).
- `src/download_openfoodfacts_sample.py` — загрузка сэмпла OpenFoodFacts адекватного размера.
- `src/preprocess_openfoodfacts.py` — предобработка и формирование признаков для кластеризации.
- `src/kmeans_clustering.py` — подбор K и кластеризация KMeans.
- `src/run_pipeline.py` — последовательный запуск всех этапов.
- `config.yaml` — настройки источника данных, Spark и KMeans.

## Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> Для Windows можно использовать `py -3.11` вместо `python3`.
> Подробная настройка PySpark на Windows:  
> https://sparkbyexamples.com/pyspark/how-to-install-and-run-pyspark-onwindows/

## Запуск проверки Spark (WordCount)

```bash
python3 src/wordcount.py
```

Результат сохраняется в `data/output/wordcount_result`.

## Запуск пайплайна кластеризации

Пошагово:

```bash
python3 src/download_openfoodfacts_sample.py
python3 src/preprocess_openfoodfacts.py
python3 src/kmeans_clustering.py
```

Или всё сразу:

```bash
python3 src/run_pipeline.py
```

## Выходные артефакты

- `data/raw/openfoodfacts_sample.jsonl` — сэмпл исходных данных.
- `data/processed/openfoodfacts_features.parquet` — очищенные признаки.
- `data/output/openfoodfacts_clusters.parquet` — кластерные предсказания.
- `data/output/cluster_metrics.json` — метрики (silhouette для каждого K).
- `data/output/cluster_centers.json` — центры кластеров.

## Сборка дистрибутива

```bash
python3 scripts/make_distribution.py
```

Архив: `dist/lab5_distribution.zip`.
