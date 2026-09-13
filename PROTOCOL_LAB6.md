# Протокол взаимодействия модели и источника данных (ЛР6)

## Участники

- **Источник данных:** MongoDB.
- **Модель:** PySpark-пайплайн кластеризации KMeans.

## Коллекции MongoDB

1. `openfoodfacts_source` — исходные записи для обучения/инференса.
2. `kmeans_results` — результаты модели (кластер для каждого продукта).
3. `pipeline_runs` — служебные статусы запусков пайплайна.

## Формат исходных данных (`openfoodfacts_source`)

```json
{
  "code": "3017620422003",
  "product_name": "Nutella",
  "nutriments": {
    "energy-kcal_100g": 539.0,
    "fat_100g": 30.9,
    "carbohydrates_100g": 57.5,
    "sugars_100g": 56.3,
    "proteins_100g": 6.3,
    "salt_100g": 0.107
  },
  "source": "openfoodfacts"
}
```

## Формат результатов (`kmeans_results`)

```json
{
  "run_id": "lab6-run-20260913T170000Z",
  "processed_at": "2026-09-13T17:00:15.000000+00:00",
  "model": "kmeans",
  "best_k": 4,
  "best_silhouette": 0.6191,
  "code": "3017620422003",
  "product_name": "Nutella",
  "features": {
    "energy_kcal_100g": 539.0,
    "fat_100g": 30.9,
    "carbohydrates_100g": 57.5,
    "sugars_100g": 56.3,
    "proteins_100g": 6.3,
    "salt_100g": 0.107
  },
  "cluster_id": 1
}
```

## Этапы протокола

1. `started` — запуск пайплайна.
2. `bootstrapped` — (опционально) первичное заполнение source-коллекции.
3. `exported` — выгрузка source-данных из MongoDB в `data/raw/openfoodfacts_sample.jsonl`.
4. `completed` — модель отработала, результаты загружены в `kmeans_results`.

Статусы и метаданные запуска сохраняются в `pipeline_runs` по ключу `run_id`.

## Гарантии в рамках ЛР6

- При **каждом запуске** выполняется выгрузка исходных данных из MongoDB.
- Сразу после завершения модели выполняется загрузка результатов обратно в MongoDB.
