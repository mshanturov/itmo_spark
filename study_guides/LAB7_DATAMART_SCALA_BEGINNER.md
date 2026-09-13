# ЛР7 — Витрина данных на Scala Spark (очень подробное объяснение)

> Контур: `itmo_spark`, развитие ЛР6

---

## 0. Главная мысль ЛР7

В ЛР6 модель работала с источником через pipeline, но всё ещё была близко к данным источника.

ЛР7 вводит отдельный слой — **витрину данных (data mart)**:
- источник (MongoDB) -> витрина -> модель -> витрина -> источник.

Ключевое требование: модель **не должна** общаться с источником напрямую.

---

## 1. Теория: зачем нужна витрина

Data mart полезен, когда:
- нужно единое представление данных,
- нужно переносить preprocessing из модели в инфраструктурный слой,
- нужно обслуживать несколько моделей из одного источника.

Именно это и сделано в ЛР7.

---

## 2. Что добавлено технически

## Scala-проект витрины
Каталог `datamart/`:
- `build.sbt`
- `conf/datamart-local.conf`
- `conf/datamart-docker.conf`
- `src/main/scala/itmo/lab7/*`

## Python orchestration
`src/lab7_mart_pipeline.py`:
- запускает `prepare` витрины,
- запускает модель KMeans,
- запускает `publish` витрины.

---

## 3. Разбор Scala-части

## `Main.scala`
Это CLI-вход витрины.

Два режима:
- `prepare`
- `publish`

Парсинг параметров:
```scala
prepare --config <path> --run-id <id> [--bootstrap-if-empty ...]
publish --config <path> --run-id <id>
```

## `DataMartService.scala`

### prepareModelInput
1. пишет run-state `mart_started`;
2. при пустом source и флаге bootstrap наполняет источник;
3. читает source из Mongo;
4. применяет preprocessing bounds;
5. пишет parquet для модели;
6. пишет подготовленные записи в коллекцию витрины;
7. пишет run-state `mart_prepared`.

### publishModelResults
1. читает model metrics json;
2. читает parquet с предсказаниями;
3. пишет документы в `kmeans_results`;
4. пишет run-state `mart_completed`.

## `MongoGateway.scala`
Это слой доступа к MongoDB на Scala:
- чтение source,
- bootstrap,
- запись mart,
- запись результатов,
- обновление статусов.

---

## 4. Разбор Python orchestration `src/lab7_mart_pipeline.py`

Класс `Lab7Pipeline`:
- генерирует `run_id`;
- запускает sbt-команду `runMain itmo.lab7.Main ...`;
- при необходимости делает bootstrap sample;
- запускает kmeans job.

Ключевой момент:
```python
self._run_datamart_command(prepare_args)
self._kmeans_job.run()
self._run_datamart_command(publish_args)
```

Это и есть связка “витрина -> модель -> витрина”.

---

## 5. Что значит “предобработка на стороне витрины”

В ЛР5 preprocessing делал Python-модуль модели.
В ЛР7 preprocessing по диапазонам реализован в Scala (`DataMartService.applyPreprocessingBounds`).

Плюс:
- модель становится проще,
- контроль качества данных централизован.

---

## 6. Проверка работоспособности

Минимум:
1. `sbt compile` в `datamart`.
2. `python3 -m src.lab7_mart_pipeline --help`.
3. E2E с MongoDB и sbt runtime.

Если e2e зелёный:
- появляется parquet для модели,
- в Mongo есть `openfoodfacts_mart_features` и `kmeans_results`.

---

## 7. Что важно показать на защите

1. Витрина действительно на **Scala**.
2. Есть отдельные режимы `prepare` / `publish`.
3. Модель не ходит в источник напрямую.
4. Есть протокол `PROTOCOL_LAB7.md`.
5. Есть docker-контур и отчёт.

---

## 8. Частые ошибки

- ❌ Витрина формальная, но модель всё равно читает source.
- ❌ Нет единого формата между слоями.
- ❌ Нет run-id и состояния протокола.

У тебя: run-id и состояния есть, формат фиксирован, связка через витрину реализована.

---

## 9. Короткий текст для защиты

«В ЛР7 я реализовал витрину данных на Scala Spark и встроил её в контур между моделью и MongoDB. Витрина берёт данные из источника, выполняет предобработку, формирует единый формат для модели, а после расчёта кластеров публикует результаты обратно в источник. Модель в этом контуре больше не взаимодействует с MongoDB напрямую.»
