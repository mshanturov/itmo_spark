# ITMO Big Data Infrastructure — Lab 8 (Migration to Kubernetes)

Лабораторная работа №8: миграция контура
**модель (ЛР5) — источник (ЛР6) — витрина (ЛР7)** в Kubernetes.

## Что реализовано

- Kubernetes/Helm инфраструктура для Spark вычислений с репликацией worker-нод.
- Поэтапный rollout:
  1. запуск модели ЛР5 в k8s,
  2. подключение источника MongoDB (ЛР6) и обновление model-контура,
  3. подключение Scala Spark витрины (ЛР7) и финальная схема model-mart-source.
- Единый runner для этапов миграции: `src/lab8_k8s_runner.py`.
- Отдельный Docker image для k8s (`docker/lab8/Dockerfile`) с Python + Java + sbt.
- Helm chart `helm/lab8-migration` для управления миграцией.

## Основные каталоги

- `src/` — модельные и orchestration скрипты.
- `datamart/` — Scala Spark витрина данных.
- `configs/` — app/spark/mongo/k8s конфигурация.
- `helm/lab8-migration/` — Helm chart для поэтапной миграции.
- `k8s/lab8/README.md` — практический rollout-гайд.

## Локальная разработка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Проверка синтаксиса Python:
```bash
python3 -m compileall src
```

Проверка компиляции Scala витрины:
```bash
cd datamart
sbt compile
```

## Сборка образа для Kubernetes

```bash
docker build -f docker/lab8/Dockerfile -t mshanturov/itmo-spark-platform:lab8 .
docker push mshanturov/itmo-spark-platform:lab8
```

## Поэтапная миграция в Kubernetes

### Шаг 1 — Модель ЛР5

```bash
helm upgrade --install lab8 ./helm/lab8-migration \
  -f ./helm/lab8-migration/values-lab5.yaml \
  --set image.repository=mshanturov/itmo-spark-platform \
  --set image.tag=lab8
```

### Шаг 2 — Источник ЛР6 + обновление модели

```bash
helm upgrade --install lab8 ./helm/lab8-migration \
  -f ./helm/lab8-migration/values-lab6.yaml \
  --set image.repository=mshanturov/itmo-spark-platform \
  --set image.tag=lab8
```

### Шаг 3 — Витрина ЛР7 + обновление контура

```bash
helm upgrade --install lab8 ./helm/lab8-migration \
  -f ./helm/lab8-migration/values-lab7.yaml \
  --set image.repository=mshanturov/itmo-spark-platform \
  --set image.tag=lab8
```

Проверка статуса:
```bash
kubectl get pods
kubectl logs job/lab8-stage-runner
kubectl get svc spark-master
kubectl get statefulset mongodb
```

## Ресурсная оптимизация

- Настраиваются `requests/limits` для runner, Spark master/worker, MongoDB.
- Репликация Spark регулируется `spark.worker.replicas`.
- Опционально включается HPA для worker-ов.
- Для упрощения можно использовать `emptyDir`; для устойчивости — PVC.

## Дистрибутив

```bash
python3 scripts/make_distribution.py --lab 8
```

Архив: `dist/lab8_distribution.zip`.
