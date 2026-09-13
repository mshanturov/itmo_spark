# ЛР2 — Интеграция модели с Redis (очень подробное объяснение)

> База: продолжается проект ЛР1 (`classic_developer_ml_cicle`)

---

## 0. Что изменилось по сравнению с ЛР1

В ЛР1 модель работала «сама по себе». В ЛР2 она стала работать с внешним источником данных — Redis.

Теперь есть сценарии:
1. сохранить входной запрос в Redis,
2. сделать предсказание,
3. сохранить предсказание в Redis,
4. получить предсказание по `request_id`,
5. сделать предсказание *из* Redis-ключа (`predict/from-redis`).

Это уже похоже на реальную сервисную архитектуру.

---

## 1. Теория коротко: почему Redis

Redis — key-value хранилище:
- очень быстрый доступ,
- простая схема ключ→значение,
- удобно для временных запросов и результатов.

Для учебной задачи идеально: минимум накладных сложностей, максимум пользы для понимания API + storage.

---

## 2. Как бы это разрабатывалось по шагам

### Шаг 1. Вынести хранилище в отдельный слой
Создаётся `src/prediction_store.py`.

Там вводится протокол `PredictionStore`:
```python
class PredictionStore(Protocol):
    def save_inference_request(...): ...
    def get_inference_request(...): ...
    def save_prediction(...): ...
    def get_prediction(...): ...
```

Смысл: API не зависит от конкретной БД.

### Шаг 2. Сделать 2 реализации
- `InMemoryPredictionStore` — для unit-тестов.
- `RedisPredictionStore` — для runtime/compose/CD.

Это важный инженерный приём: тестируемость без поднятия Redis.

### Шаг 3. Подключить store в API
`InferenceService` в `api_service.py` получает store как dependency.

### Шаг 4. Добавить новые endpoint
- `POST /inference-requests/{request_key}`
- `POST /predict/from-redis`
- `GET /predictions/{request_id}`

### Шаг 5. Добавить seed-скрипт
`src/seed_redis_data.py` наполняет Redis тестовыми входами.

### Шаг 6. Обновить functional test
`src/functional_api_test.py` проверяет новый поток end-to-end.

---

## 3. Разбор кода “для новичка”

## `src/prediction_store.py`

### InMemory реализация
Для тестов хранит всё в dict:
```python
self._inference_requests: dict[str, dict[str, float]] = {}
self._predictions: dict[str, dict[str, object]] = {}
```

### Redis реализация
Подключение:
```python
self._client = Redis.from_url(self.redis_url, decode_responses=True)
self._client.ping()
```

Если `ping` не проходит — кидается `StoreError`, и API отдаёт 503.

### Генерация ключей
```python
def _request_key(self, request_key: str) -> str:
    return f"{self.key_prefix}:inference_request:{request_key}"
```

Это важная практика неймспейсов, чтобы не смешивать разные типы сущностей.

---

## `src/api_service.py` (логика ЛР2)

Ключевая идея — метод `predict_and_store`, который:
1. валидирует вход,
2. делает predict,
3. сохраняет результат через store,
4. возвращает `request_id`.

Почему это хорошо:
- единая точка бизнес-логики;
- endpoint-ы тонкие и читаемые.

---

## `src/functional_api_test.py`

Функциональный тест делает:
1. `/health`
2. `/predict`
3. `GET /predictions/{id}`
4. запись входа через `/inference-requests/{key}`
5. `/predict/from-redis`
6. проверку 422 на невалидный payload

Это уже тест “как пользователь реально использует API”.

---

## 4. Как понять, что ЛР2 реально работает

Признаки успешной работы:
- Redis поднялся;
- API отвечает 200 на `/health`;
- POST `/predict` возвращает `request_id`;
- GET `/predictions/{request_id}` возвращает сохранённый объект;
- POST `/predict/from-redis` делает prediction по сохранённому входу.

---

## 5. Почему это полезно архитектурно

ЛР2 даёт базу для следующих лаб:
- в ЛР3 сюда добавляются безопасные секреты;
- в ЛР4 добавляется eventing (Kafka);
- но API-слой уже не ломается, потому что работа с хранилищем вынесена в адаптер.

---

## 6. Что показать преподавателю

1. `prediction_store.py` (протокол + 2 backend-а)
2. `api_service.py` (новые endpoint)
3. `functional_api_test.py`
4. `docker-compose.yml` с Redis
5. `CI/Jenkinsfile` + `CD/Jenkinsfile`
6. `REPORT_LAB2.md`

---

## 7. Типовые ошибки

- ❌ Хардкод Redis URL в коде
- ❌ API напрямую пишет в Redis без абстракции
- ❌ Нет теста на `predict/from-redis`

У тебя это закрыто через:
- env-конфиг,
- `PredictionStore` abstraction,
- отдельные endpoint + functional scenario.

---

## 8. Короткий текст для защиты

«В ЛР2 я интегрировал модельный API с Redis через отдельный слой доступа к данным. Реализованы запись входных запросов, хранение и получение предсказаний, а также сценарий предсказания по запросу, заранее сохранённому в Redis. Архитектура разделена на бизнес-логику и хранилище, что обеспечивает тестируемость и расширяемость.»
