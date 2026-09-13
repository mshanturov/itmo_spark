# Протокол миграции и взаимодействия сервисов (ЛР8)

## 1. Цель протокола

Обеспечить управляемую миграцию контура ЛР5/ЛР6/ЛР7 в Kubernetes
с контролируемыми этапами обновления.

## 2. Участники

- **Model Service (ЛР5)**: PySpark модель кластеризации.
- **Source Service (ЛР6)**: MongoDB.
- **Data Mart Service (ЛР7)**: Scala Spark витрина.
- **Spark Infrastructure**: spark-master + spark-worker (N реплик).

## 3. Этапы миграции

### Stage `lab5`
- Поднимается Spark-инфраструктура.
- Запускается model pipeline (`src.lab8_k8s_runner --stage lab5`).
- Проверяется выполнение кластеризации в k8s.

### Stage `lab6`
- Разворачивается MongoDB (StatefulSet).
- Модель обновляется до Mongo-backed сценария (`--stage lab6`).
- Выполняется выгрузка source и загрузка результатов в MongoDB.

### Stage `lab7`
- Подключается Scala Spark data mart (`--stage lab7`).
- Data mart выполняет `prepare` и `publish`.
- Модель не взаимодействует с источником напрямую,
  а читает подготовленный витриной parquet.

## 4. Контракт запуска

Helm value `stage` определяет сценарий выполнения Job `lab8-stage-runner`:

- `stage=lab5` -> `run_lab5_stage`
- `stage=lab6` -> `run_lab6_stage`
- `stage=lab7` -> `run_lab7_stage`

Каждый `helm upgrade` инициирует новый запуск через post-upgrade hook.

## 5. Контракт данных

- `openfoodfacts_source` — источник документов продуктов.
- `openfoodfacts_mart_features` — предобработанные данные витрины.
- `kmeans_results` — результаты кластеризации.
- `pipeline_runs` / `mart_runs` — статусы выполнения пайплайнов.

## 6. Оптимизация ресурсов

- Spark workers масштабируются числом реплик или HPA.
- Для runner-а доступны `emptyDir` или PVC.
- Ограничения CPU/RAM заданы в Helm values для всех сервисов.
