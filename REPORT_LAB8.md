# Отчёт по лабораторной работе №8

**Дисциплина:** Инфраструктура больших данных  
**Тема:** Миграция на Kubernetes

## 1. Цель

Получить навыки оркестрации контейнеров с использованием Kubernetes
путём миграции сервиса модели на PySpark, сервиса витрины на Spark
и сервиса источника данных.

## 2. Выполненные шаги

### 2.1 Инфраструктура Spark + репликация

Реализован Helm chart `helm/lab8-migration`, который поднимает:
- `spark-master` (Deployment + Service),
- `spark-worker` (Deployment) с параметром `replicas` и опциональным HPA.

### 2.2 Запуск сервиса модели (ЛР5) в k8s

Добавлен единый runner `src/lab8_k8s_runner.py`.
При `stage=lab5` выполняется:
- загрузка sample,
- предобработка,
- KMeans кластеризация.

### 2.3 Подключение источника (ЛР6) и обновление модели

При `stage=lab6` chart дополнительно разворачивает MongoDB (StatefulSet),
а runner запускает Mongo-backed pipeline (`src/lab6_mongo_pipeline.py`).

### 2.4 Подключение витрины (ЛР7) и обновление контура

Добавлена Scala Spark витрина (`datamart/`) и её k8s-конфиг.
При `stage=lab7` runner выполняет последовательность:
1. `prepare` витрины,
2. запуск модели,
3. `publish` витрины.

Итоговая схема: **модель — витрина — источник**.

### 2.5 Оптимизация утилизации ресурсов

- Настроены requests/limits для Spark, MongoDB и runner-а.
- Добавлена возможность HPA для Spark worker-ов.
- Добавлен выбор storage-режима: `emptyDir` или PVC.

## 3. Файлы, добавленные для ЛР8

- `docker/lab8/Dockerfile`
- `src/lab8_k8s_runner.py`
- `configs/spark_config_k8s.yaml`
- `configs/mongo_config_k8s.yaml`
- `configs/lab7_pipeline_k8s.yaml`
- `datamart/conf/datamart-k8s.conf`
- `helm/lab8-migration/*`
- `k8s/lab8/README.md`
- `PROTOCOL_LAB8.md`

## 4. Проверка

Локально в агенте проверены:
- компиляция Python модулей (`python3 -m compileall src`),
- компиляция Scala витрины (`sbt compile`).

Ограничение среды: отсутствует доступ к Docker/Kubernetes runtime,
поэтому e2e запуск кластера выполняется по инструкциям в `k8s/lab8/README.md`.

## 5. Результаты

1. Отчёт о проделанной работе — данный файл.
2. Ссылка на репозиторий GitHub — `https://github.com/mshanturov/itmo_spark`.
3. Актуальный дистрибутив — `dist/lab8_distribution.zip`.
