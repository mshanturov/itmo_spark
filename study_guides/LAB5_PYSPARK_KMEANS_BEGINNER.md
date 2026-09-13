# ЛР5 — Кластеризация на PySpark (очень подробное объяснение)

> Контур: репозиторий `itmo_spark`, ветка `main` (база ЛР5)

---

## 0. Что ты сделал в ЛР5

Ты построил Spark-пайплайн, который:
1. проверяет Spark на WordCount,
2. скачивает сэмпл OpenFoodFacts,
3. выделяет признаки и чистит данные,
4. обучает KMeans на разных `K`,
5. выбирает лучший `K` по silhouette,
6. сохраняет артефакты (parquet + json).

Это уже не «один ноутбук», а инженерный multi-step pipeline.

---

## 1. Теория: что такое KMeans и silhouette

### KMeans
Алгоритм делит объекты на `K` кластеров так, чтобы точки внутри кластера были похожи.

### Почему нужен подбор `K`
Если `K` слишком мало — группы грубые.
Если `K` слишком много — кластеры шумные.

### Silhouette
Метрика качества кластеризации (чем выше, тем лучше, обычно до 1).

---

## 2. Архитектура ЛР5

## Настройки
- `src/settings.py` — dataclass и загрузка YAML-конфигов.
- `src/spark_session_factory.py` — создание SparkSession.
- `configs/app_config.yaml` — данные и параметры kmeans.
- `configs/spark_config.yaml` — Spark-конфиг отдельно.

## Фичи
- `src/openfoodfacts_features.py` — список признаков, диапазоны, rationale.

## Job-слои
- `src/wordcount.py` — `WordCountJob`
- `src/download_openfoodfacts_sample.py` — `OpenFoodFactsSampler`
- `src/preprocess_openfoodfacts.py` — `OpenFoodFactsPreprocessor`
- `src/kmeans_clustering.py` — `KMeansClusteringJob`
- `src/run_pipeline.py` — orchestration всех шагов.

---

## 3. Разбор кода по шагам

## `src/settings.py`

Идея: не кидать словари по всему проекту, а загрузить config в typed-объекты.

```python
@dataclass(frozen=True)
class ClusteringSettings:
    k_values: list[int]
    seed: int
    max_iter: int
```

Это уменьшает ошибки и делает код читаемым.

## `src/spark_session_factory.py`

```python
builder = SparkSession.builder.appName(app_name).master(self._settings.master)
for key, value in self._settings.default_configs.items():
    builder = builder.config(key, value)
```

Почему хорошо:
- все Spark-параметры централизованы в yaml;
- проще перенос в ЛР8 (k8s).

## `src/wordcount.py`

WordCount нужен как sanity check Spark.

Ключ:
```python
F.explode(F.split(F.regexp_replace(F.lower(F.col("value")), r"[^a-zа-я0-9\s]", ""), r"\s+"))
```

Это:
1. lower-case,
2. удаление пунктуации,
3. split,
4. разворачивание массива слов в строки.

## `src/download_openfoodfacts_sample.py`

Скачивает **часть** огромного jsonl.gz, чтобы не убить локальные ресурсы.

## `src/preprocess_openfoodfacts.py`

Здесь важный паттерн: явная схема json.

```python
parsed_df = raw_df.select(F.from_json(F.col("value"), OPENFOODFACTS_SCHEMA).alias("item")).select("item.*")
```

Так ты избежал проблем с конфликтующими полями в «грязном» JSON.

Дальше:
- dropna по обязательным фичам,
- фильтрация по диапазонам `FEATURE_BOUNDS`.

## `src/kmeans_clustering.py`

1) собирает вектор признаков,
2) стандартизирует,
3) перебирает `k_values`,
4) считает silhouette,
5) выбирает лучший кластеризатор.

Ключ:
```python
for k in self._settings.clustering.k_values:
    model = KMeans(k=k, ...).fit(scaled_df)
    score = float(evaluator.evaluate(predictions))
```

Сохраняются:
- `openfoodfacts_clusters.parquet`
- `cluster_metrics.json`
- `cluster_centers.json`

---

## 4. Почему выбраны именно эти признаки

Использованы признаки на 100 г:
- `energy_kcal_100g`
- `fat_100g`
- `carbohydrates_100g`
- `sugars_100g`
- `proteins_100g`
- `salt_100g`

Причины:
1. числовые, подходят KMeans;
2. покрывают базовый нутриентный профиль;
3. доступны у большого числа продуктов.

---

## 5. Как запускать и читать результат

```bash
python3 -m src.wordcount
python3 -m src.download_openfoodfacts_sample
python3 -m src.preprocess_openfoodfacts
python3 -m src.kmeans_clustering
```

или всё сразу:
```bash
python3 -m src.run_pipeline
```

Смотри файлы:
- `data/output/cluster_metrics.json` — лучший `K` и silhouette,
- `data/output/openfoodfacts_clusters.parquet` — кластер каждого продукта.

---

## 6. Типичные проблемы

- Ошибка импорта `src` при запуске скрипта напрямую.
- Слишком большой объём данных.
- Отсутствие Java/Spark runtime.
- Некорректный JSON schema inference.

Ты это закрыл:
- fallback по путям,
- сэмплирование,
- явная схема.

---

## 7. Что показать преподавателю

1. WordCount как проверка Spark.
2. preprocess с явной схемой.
3. подбор `K` и метрики silhouette.
4. `configs/spark_config.yaml` отдельно.
5. OOP-декомпозиция job-классов.

---

## 8. Короткий текст для защиты

«В ЛР5 я реализовал Spark-пайплайн кластеризации KMeans: проверка Spark через WordCount, сэмплирование OpenFoodFacts, предобработка с явной схемой и фильтрацией признаков, обучение KMeans с подбором лучшего K по silhouette. Конфигурация Spark вынесена отдельно, логика разделена на OOP-компоненты, результаты сохранены в parquet и json.»
