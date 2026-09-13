# Lab 8 Kubernetes rollout (model -> source -> data mart)

Этот каталог описывает поэтапную миграцию в k8s:

1. `lab5`: только модель на PySpark + Spark cluster.
2. `lab6`: добавляется MongoDB как источник и обновляется модельный контур.
3. `lab7`: добавляется Scala Spark data mart, модель и источник работают через витрину.

## Предусловия

- Kubernetes cluster (minikube/kind/managed).
- Helm 3.
- Docker image с проектом, собранный из `docker/lab8/Dockerfile`.

## Сборка и публикация образа

```bash
docker build -f docker/lab8/Dockerfile -t mshanturov/itmo-spark-platform:lab8 .
docker push mshanturov/itmo-spark-platform:lab8
```

## Шаг 1. Запуск ЛР5 в k8s

```bash
helm upgrade --install lab8 ./helm/lab8-migration \
  -f ./helm/lab8-migration/values-lab5.yaml \
  --set image.repository=mshanturov/itmo-spark-platform \
  --set image.tag=lab8
```

Проверка:
```bash
kubectl get pods
kubectl logs job/lab8-stage-runner
kubectl get svc spark-master
```

## Шаг 2. Подключение источника ЛР6

```bash
helm upgrade --install lab8 ./helm/lab8-migration \
  -f ./helm/lab8-migration/values-lab6.yaml \
  --set image.repository=mshanturov/itmo-spark-platform \
  --set image.tag=lab8
```

Проверка:
```bash
kubectl get statefulset mongodb
kubectl logs statefulset/mongodb
kubectl logs job/lab8-stage-runner
```

## Шаг 3. Подключение витрины ЛР7

```bash
helm upgrade --install lab8 ./helm/lab8-migration \
  -f ./helm/lab8-migration/values-lab7.yaml \
  --set image.repository=mshanturov/itmo-spark-platform \
  --set image.tag=lab8
```

Проверка:
```bash
kubectl logs job/lab8-stage-runner
kubectl get deploy spark-worker
```

## Ресурсная оптимизация

- Spark workers масштабируются через `spark.worker.replicas`.
- Можно включить HPA: `spark.worker.autoscaling.enabled=true`.
- Для экономии диска можно оставить `storage.usePersistentVolume=false` и использовать `emptyDir`.
- Для устойчивости пайплайна можно включить PVC (`storage.usePersistentVolume=true`).
