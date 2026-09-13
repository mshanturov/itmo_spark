# Протокол взаимодействия (ЛР7): модель ↔ витрина ↔ источник

## 1. Компоненты

- **Источник данных:** MongoDB (`openfoodfacts_source`).
- **Витрина данных:** Scala Spark-приложение (`datamart`, класс `itmo.lab7.Main`).
- **Модель:** PySpark KMeans (`src/kmeans_clustering.py`).

Модель **не обращается к MongoDB напрямую**. Обмен идёт только через витрину.

## 2. Контракты данных

### 2.1 Source-формат (`openfoodfacts_source`)

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

### 2.2 Формат витрины (`openfoodfacts_mart_features`)

```json
{
  "run_id": "lab7-run-20260913T172200Z",
  "prepared_at": "2026-09-13T17:22:08Z",
  "code": "3017620422003",
  "product_name": "Nutella",
  "features": {
    "energy_kcal_100g": 539.0,
    "fat_100g": 30.9,
    "carbohydrates_100g": 57.5,
    "sugars_100g": 56.3,
    "proteins_100g": 6.3,
    "salt_100g": 0.107
  }
}
```

### 2.3 Формат для модели (parquet)

Витрина формирует `data/processed/openfoodfacts_features.parquet` с колонками:
- `code`, `product_name`
- `energy_kcal_100g`, `fat_100g`, `carbohydrates_100g`, `sugars_100g`, `proteins_100g`, `salt_100g`

### 2.4 Формат результата (`kmeans_results`)

```json
{
  "run_id": "lab7-run-20260913T172200Z",
  "processed_at": "2026-09-13T17:22:20Z",
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

## 3. Состояния протокола (`mart_runs`)

- `mart_started`
- `mart_bootstrapped` (если источник пуст и включён bootstrap)
- `mart_prepared`
- `mart_completed`

Каждое состояние связано с `run_id` и метаданными (`source_rows`, `prepared_rows`, `loaded_rows` и т.д.).

## 4. Последовательность выполнения

1. Витрина читает данные из `openfoodfacts_source`.
2. Витрина выполняет предобработку (фильтрация диапазонов признаков).
3. Витрина записывает единый формат в `openfoodfacts_mart_features` и parquet для модели.
4. Модель запускает KMeans на parquet от витрины.
5. Витрина читает предсказания модели и загружает их в `kmeans_results`.
