# Отчёт по лабораторной работе №7

**Дисциплина:** Инфраструктура больших данных  
**Тема:** Витрина данных на Spark

## 1. Цель работы

Получить навыки разработки витрины данных и её интеграции в существующий контур
`модель (ЛР5) — источник данных (ЛР6)`.

## 2. Выполненные задачи

1. Разработана витрина данных на **Scala + Spark**.
2. Реализован единый протокол взаимодействия между моделью и источником через витрину.
3. Предобработка данных перенесена на сторону витрины данных.
4. Выполнена интеграция по схеме `модель — витрина — источник`.

## 3. Архитектура

- **Источник:** MongoDB (`openfoodfacts_source`)
- **Витрина (Scala Spark):**
  - чтение source-данных из MongoDB,
  - фильтрация и нормализация формата,
  - запись в `openfoodfacts_mart_features` и parquet для модели,
  - загрузка результатов модели в `kmeans_results`.
- **Модель (PySpark KMeans):**
  - читает только подготовленный parquet от витрины,
  - считает кластеры и метрики,
  - не обращается к MongoDB напрямую.

## 4. Протокол и формат

Подробно описаны в `PROTOCOL_LAB7.md`:
- форматы source/mart/result документов,
- состояния протокола (`mart_started`, `mart_prepared`, `mart_completed`),
- последовательность вызовов `prepare -> model -> publish`.

## 5. Техническая реализация

### Scala витрина
- `datamart/src/main/scala/itmo/lab7/Main.scala`
- `datamart/src/main/scala/itmo/lab7/service/DataMartService.scala`
- `datamart/src/main/scala/itmo/lab7/mongo/MongoGateway.scala`
- `datamart/src/main/scala/itmo/lab7/model/FeatureRecord.scala`
- `datamart/src/main/scala/itmo/lab7/config/DataMartConfig.scala`

### Python оркестратор
- `src/lab7_mart_pipeline.py` — запуск полного контура.

### Конфигурация
- `datamart/conf/datamart-local.conf`
- `datamart/conf/datamart-docker.conf`
- `configs/lab7_pipeline.yaml`

## 6. Docker интеграция

Добавлены:
- `docker/lab7/Dockerfile`
- `docker-compose.lab7.yml`

Позволяет запускать схему `модель-витрина-источник` в контейнерах.

## 7. Проверка

Проверены:
- корректность Python-части Spark и модели;
- CLI-параметры пайплайна и новые конфиги;
- сборка дистрибутива `dist/lab7_distribution.zip`.

> В текущем окружении агента Docker/SBT runtime для полного e2e запуска недоступен,
> поэтому для финальной демонстрации используется запуск на локальной машине или CI.

## 8. Результат

- Реализована витрина данных на Scala Spark.
- Удалена прямая связка модели с источником в контуре ЛР7.
- Модель получает только предобработанные данные из витрины.
- Результаты модели публикуются в источник через витрину.

## 9. Ссылки

- Репозиторий GitHub: `https://github.com/mshanturov/itmo_spark`
- Ветка ЛР7: `lab7`
- Дистрибутив: `dist/lab7_distribution.zip`
