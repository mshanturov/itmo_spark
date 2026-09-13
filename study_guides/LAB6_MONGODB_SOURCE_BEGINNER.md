# ЛР6 — Источник данных MongoDB для PySpark-модели (очень подробное объяснение)

> Контур: `itmo_spark`, развитие ЛР5

---

## 0. Что принципиально добавила ЛР6

В ЛР5 источник был «файл на диске». В ЛР6 источник стал внешним сервисом — MongoDB.

Требования ЛР6:
1. выгружать source-данные при каждом запуске;
2. загружать результаты сразу после модели;
3. описать протокол взаимодействия;
4. определить формат хранения.

Все это реализовано в `src/lab6_mongo_pipeline.py` + `src/mongo_protocol.py`.

---

## 1. Теория: зачем отдельный protocol слой

Если модель напрямую начнёт делать Mongo-операции в разных местах, код быстро станет «кашей».

Поэтому сделан отдельный слой:
- `MongoProtocolClient` отвечает только за Mongo;
- pipeline оркестрирует шаги.

Плюсы:
- проще тестировать;
- проще менять источник;
- понятный контракт.

---

## 2. Пошаговая логика ЛР6

### Шаг 1. Описание конфигурации Mongo
`configs/mongo_config.yaml`:
- URI,
- БД,
- имена коллекций,
- pipeline-параметры (`bootstrap_lines`, `write_batch_size`).

### Шаг 2. Реализация клиента протокола
`src/mongo_protocol.py`:
- `update_run_state`
- `source_is_empty`
- `bootstrap_source_from_jsonl`
- `export_source_to_jsonl`
- `load_predictions`

### Шаг 3. Реализация orchestration
`src/lab6_mongo_pipeline.py`:
- генерирует `run_id`,
- при пустом source может сделать bootstrap,
- всегда экспортирует source в JSONL,
- гоняет preprocess + kmeans,
- грузит results в Mongo.

### Шаг 4. Проверка состояния
`src/check_mongo_results.py` печатает count по коллекциям и latest run.

---

## 3. Разбор кода: `src/mongo_protocol.py`

## dataclass настроек
```python
@dataclass(frozen=True)
class MongoSettings:
    connection: MongoConnectionSettings
    collections: MongoCollectionSettings
    pipeline: MongoPipelineSettings
```

## проверка подключения
```python
client = MongoClient(...)
client.admin.command("ping")
```

Это fail-fast: если источник недоступен — лучше падать сразу и понятно.

## экспорт source
```python
cursor = db[self._settings.collections.source].find({}, {"_id": 0, "code": 1, ...})
```

Важный момент: `_id` не тащится в модельный слой.

## загрузка результатов
`load_predictions` пишет документы пакетами (`write_batch_size`).
Это защищает от больших одномоментных вставок.

---

## 4. Разбор кода: `src/lab6_mongo_pipeline.py`

Основная последовательность:

1. `update_run_state(run_id, "started")`
2. bootstrap (опционально)
3. `export_source_to_jsonl(...)`
4. `OpenFoodFactsPreprocessor.run()`
5. `KMeansClusteringJob.run()`
6. чтение metrics и predictions
7. `load_predictions(...)`
8. `update_run_state(run_id, "completed", ...)`

То есть ЛР6 — уже настоящий data workflow, а не одиночный скрипт.

---

## 5. Формат коллекций (важно для объяснения)

- `openfoodfacts_source`: исходные продукты (code, name, nutriments)
- `kmeans_results`: результат модели + метрики + cluster_id
- `pipeline_runs`: статусы/метаданные запусков

Это и есть «протокол на уровне данных».

---

## 6. Docker-контур

`docker-compose.yml` (ЛР6-репо) поднимает:
- `mongodb`
- `model-pipeline`

Команда:
```bash
docker compose up --build model-pipeline
```

После run можно выполнить:
```bash
python3 -m src.check_mongo_results
```

---

## 7. Почему это шаг вперёд

ЛР5: file-based pipeline.
ЛР6: source-driven pipeline с внешней БД и протоколом состояний.

Это ближе к реальным MLOps-сценариям.

---

## 8. Что показать преподавателю

1. `PROTOCOL_LAB6.md`
2. `mongo_protocol.py`
3. `lab6_mongo_pipeline.py`
4. коллекции source/results/runs
5. docker-compose запуск

---

## 9. Типовые ошибки

- ❌ Нет run-state tracking.
- ❌ Модель пишет в БД напрямую без слоя.
- ❌ Нет строгого формата результатов.

У тебя это реализовано корректно.

---

## 10. Короткий текст для защиты

«В ЛР6 я интегрировал PySpark-модель с MongoDB как внешним источником данных. Реализован отдельный протокол взаимодействия: на каждом запуске источник выгружается, после завершения модели результаты сразу записываются обратно в MongoDB, а статус пайплайна сохраняется в коллекции запусков. Форматы source и result зафиксированы документированно.»
