# ЛР4 — Kafka Producer/Consumer + Vault + Redis (очень подробное объяснение)

> Продолжение ЛР3 в `classic_developer_ml_cicle`

---

## 0. Что нового в ЛР4

В ЛР2/ЛР3 был «запрос -> предсказание -> хранение». В ЛР4 добавляется событийная шина:

- API после предсказания публикует событие в Kafka (Producer).
- Отдельный сервис читает события из Kafka (Consumer).
- Consumer отмечает обработку события в Redis.

Это переводит систему к **event-driven подходу**.

---

## 1. Теория простыми словами

### Producer
Компонент, который отправляет сообщение в топик Kafka.

### Consumer
Компонент, который читает сообщения из топика.

Почему это круто:
- сервисы слабо связаны,
- можно масштабировать обработку,
- легко строить асинхронные процессы.

---

## 2. Поток данных (очень важно для защиты)

1. Клиент делает `POST /predict`.
2. API считает `predicted_class`.
3. API сохраняет prediction в Redis.
4. API публикует событие `prediction.created` в Kafka.
5. `kafka-consumer` получает сообщение.
6. consumer сохраняет «consumed event» в Redis.
7. `GET /consumed-events/{request_id}` подтверждает, что событие реально обработано.

---

## 3. Разбор кода

## `src/kafka_bus.py`

### Абстракция шины
```python
class PredictionEventBus(Protocol):
    def publish_prediction_event(self, event: dict[str, object]) -> None:
        ...
```

Две реализации:
- `InMemoryPredictionEventBus` (для unit-тестов),
- `KafkaPredictionEventBus` (боевой путь).

Producer создаётся так:
```python
self._producer = KafkaProducer(
    bootstrap_servers=[...],
    value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
    retries=5,
    acks="all",
)
```

`acks="all"` = повышенная надёжность записи.

## `src/kafka_consumer_service.py`

Consumer:
- запускается с ретраями, пока брокер не доступен,
- читает сообщения,
- кладёт их в store (`save_consumed_event`).

Важный блок:
```python
for message in consumer:
    event = message.value
    request_id = str(event.get("request_id", "")).strip()
    if request_id:
        store.save_consumed_event(request_id, event)
```

## `src/api_service.py`

В `predict_and_store` после сохранения prediction публикуется событие:
```python
event = self._build_prediction_event(...)
self.event_bus.publish_prediction_event(event)
```

В режиме `inmemory` есть shortcut для тестов:
```python
if self.message_bus_backend == "inmemory":
    self.prediction_store.save_consumed_event(request_id, event)
```

Это нужно, чтобы unit-тесты не зависели от реальной Kafka.

---

## 4. Docker Compose контур

`docker-compose.yml` поднимает:
- `vault-init`
- `kafka`
- `redis`
- `web` (API + producer)
- `kafka-consumer`
- `functional-tests`

Именно этот контур показывает полную сквозную работу.

---

## 5. Functional test как доказательство

`src/functional_api_test.py` проверяет:
- прямое предсказание,
- сохранение/чтение prediction,
- ожидание consumed event (`wait_for_consumed_event`),
- сценарий `predict/from-redis`,
- валидацию невалидного payload.

Если тест зелёный — producer и consumer реально связаны и работают.

---

## 6. Что поменялось в CI/CD

`CI/Jenkinsfile`:
- unit tests + coverage,
- container functional tests (Vault + Redis + Kafka),
- docker build+push.

`CD/Jenkinsfile`:
- поднимает окружение,
- seed-ит Redis,
- гоняет функциональные тесты,
- собирает логи.

---

## 7. Что показать преподавателю

1. `kafka_bus.py` (producer abstraction)
2. `kafka_consumer_service.py`
3. `api_service.py` endpoint `/consumed-events/{request_id}`
4. `functional_api_test.py` с ожиданием consumed event
5. `docker-compose.yml`
6. `REPORT_LAB4.md`

---

## 8. Типичные ошибки

- ❌ Публиковать событие, но не проверять обработку.
- ❌ Связывать consumer напрямую с API вызовом (без Kafka).
- ❌ Хардкод Kafka параметров.

У тебя решено корректно:
- есть consumer,
- есть endpoint-подтверждение,
- Kafka параметры через vault/env.

---

## 9. Короткий текст для защиты

«В ЛР4 я добавил событийную интеграцию через Kafka: API публикует события о предсказаниях, отдельный consumer их обрабатывает и сохраняет подтверждение в Redis. Сквозной путь проверяется функциональным тестом и endpoint-ом `/consumed-events/{request_id}`. Секреты Redis/Kafka остаются в Vault-контуре.»
