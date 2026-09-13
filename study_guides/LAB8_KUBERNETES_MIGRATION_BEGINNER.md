# ЛР8 — Миграция на Kubernetes (очень подробное объяснение)

> Контур: `itmo_spark`, ветка `lab8`

---

## 0. Что такое ЛР8 в одном предложении

Ты переносишь уже готовую систему из ЛР5/6/7 в Kubernetes так, чтобы она разворачивалась **поэтапно** и контролируемо.

Система состоит из:
- модельного сервиса (PySpark),
- источника данных (MongoDB),
- витрины данных (Scala Spark),
- Spark-инфраструктуры (master/worker).

---

## 1. Почему миграция делается этапами

Если перенести всё сразу, сложно понять, где ошибка.

Поэтому в ЛР8 миграция разбита на 3 стадии:
1. `lab5`: только модель + Spark-инфра,
2. `lab6`: добавить MongoDB и обновить модельный контур,
3. `lab7`: добавить витрину и финальную схему model-mart-source.

Так проще диагностировать и защищать.

---

## 2. Что конкретно добавлено в проект

## Новый runner
- `src/lab8_k8s_runner.py`

Он умеет запускать stage:
- `--stage lab5`
- `--stage lab6`
- `--stage lab7`

## K8s-конфиги
- `configs/spark_config_k8s.yaml`
- `configs/mongo_config_k8s.yaml`
- `configs/lab7_pipeline_k8s.yaml`
- `datamart/conf/datamart-k8s.conf`

## Docker image для k8s
- `docker/lab8/Dockerfile`

В image есть Python + Java + sbt, чтобы запускать и модель, и Scala-витрину.

## Helm chart
- `helm/lab8-migration/*`

Шаблоны:
- Spark master deployment/service,
- Spark worker deployment,
- HPA для worker (опционально),
- MongoDB StatefulSet (условно для stage != lab5),
- Job runner с post-install/post-upgrade hook.

---

## 3. Разбор ключевого файла `src/lab8_k8s_runner.py`

## parse_args
Принимает:
- stage,
- пути к app/spark/mongo/lab7 config,
- `--bootstrap-if-empty`.

## run_lab5_stage
- качает sample,
- preprocess,
- kmeans.

## run_lab6_stage
- вызывает `MongoBackedLab6Pipeline`.

## run_lab7_stage
- включает `SBT_BIN` по умолчанию `/opt/sbt/bin/sbt`,
- вызывает `Lab7Pipeline` с k8s-конфигом витрины.

Идея отличная для защиты: **единая точка запуска всех стадий**.

---

## 4. Разбор Helm chart простыми словами

## `values.yaml`
Содержит:
- `stage` (какой этап миграции),
- image name/tag,
- ресурсы (requests/limits),
- Spark worker replicas,
- настройки MongoDB persistence,
- режим хранения runner-data (emptyDir/PVC).

## `templates/stage-runner-job.yaml`
Это Job-хук, который запускается после install/upgrade chart:

```yaml
"helm.sh/hook": post-install,post-upgrade
```

Команда контейнера:
```yaml
python3 -m src.lab8_k8s_runner --stage <значение из values>
```

То есть, меняя `stage`, ты управляешь поведением пайплайна.

## Условный MongoDB
`templates/mongodb.yaml` включается только когда stage != lab5.

Это аккуратная реализация требования «сначала модель, потом источник».

---

## 5. Поэтапный rollout как ты бы показывал на практике

### Stage 1 (lab5)
```bash
helm upgrade --install lab8 ./helm/lab8-migration \
  -f ./helm/lab8-migration/values-lab5.yaml \
  --set image.repository=mshanturov/itmo-spark-platform \
  --set image.tag=lab8
```
Проверка:
```bash
kubectl logs job/lab8-stage-runner
```

### Stage 2 (lab6)
```bash
helm upgrade --install lab8 ./helm/lab8-migration \
  -f ./helm/lab8-migration/values-lab6.yaml ...
```
Проверка:
- появился statefulset `mongodb`;
- runner-лог показывает Mongo-backed workflow.

### Stage 3 (lab7)
```bash
helm upgrade --install lab8 ./helm/lab8-migration \
  -f ./helm/lab8-migration/values-lab7.yaml ...
```
Проверка:
- runner делает `prepare -> model -> publish`.

---

## 6. Оптимизация ресурсов (пункт задания)

В chart предусмотрено:
- `spark.worker.replicas` для простого масштабирования;
- optional HPA;
- requests/limits для всех ключевых подов;
- выбор `emptyDir` vs PVC.

Это и есть практический ответ на пункт оптимизации утилизации.

---

## 7. Как читать артефакты после запуска

- `data/processed/openfoodfacts_features.parquet` — подготовленный датасет.
- `data/output/openfoodfacts_clusters.parquet` — результат кластеризации.
- `data/output/cluster_metrics.json` — лучший `K`, silhouette.
- MongoDB коллекции:
  - `openfoodfacts_source`
  - `openfoodfacts_mart_features`
  - `kmeans_results`
  - `pipeline_runs` / `mart_runs`

---

## 8. Валидации, которые уже были сделаны

- `python3 -m compileall src`
- `sbt compile` (datamart)
- `helm template` для values lab5/lab6/lab7
- `helm lint`

Это доказывает, что инфраструктурная часть синтаксически и структурно корректна.

---

## 9. Что показать преподавателю

1. `src/lab8_k8s_runner.py` (единый stage-runner)
2. `helm/lab8-migration/values-lab5|6|7.yaml`
3. `templates/mongodb.yaml` (условное подключение источника)
4. `templates/stage-runner-job.yaml` (поэтапный запуск)
5. `PROTOCOL_LAB8.md`
6. `k8s/lab8/README.md`
7. `REPORT_LAB8.md`

---

## 10. Типичные ошибки

- ❌ Монолитная миграция «всё сразу».
- ❌ Нет разделения конфигов для k8s.
- ❌ Отсутствие limits/requests.
- ❌ Неуправляемый запуск без явной стадии.

В твоём проекте это закрыто:
- staged rollout,
- отдельные k8s config,
- ресурсные политики,
- Helm values per stage.

---

## 11. Короткий текст для защиты

«В ЛР8 я мигрировал контур ЛР5–ЛР7 в Kubernetes поэтапно через Helm. На первом этапе запускается только модельный пайплайн и Spark-инфраструктура, на втором добавляется MongoDB-источник и Mongo-backed обработка, на третьем подключается Scala Spark витрина и финальная схема model-mart-source. Для контроля и воспроизводимости реализован единый stage-runner, ресурсные лимиты, масштабирование Spark worker-ов и документированный протокол миграции.»
