# Полный учебный набор по ЛР1–ЛР8 (как для новичка)

Эта папка содержит **подробные пошаговые объяснения** всех лабораторных:

- [ЛР1 — Классический ML lifecycle](./LAB1_CLASSIC_ML_LIFECYCLE_BEGINNER.md)
- [ЛР2 — Интеграция с Redis](./LAB2_REDIS_SOURCE_BEGINNER.md)
- [ЛР3 — Секреты в Ansible Vault](./LAB3_VAULT_BEGINNER.md)
- [ЛР4 — Kafka producer/consumer](./LAB4_KAFKA_BEGINNER.md)
- [ЛР5 — KMeans на PySpark](./LAB5_PYSPARK_KMEANS_BEGINNER.md)
- [ЛР6 — Источник данных MongoDB](./LAB6_MONGODB_SOURCE_BEGINNER.md)
- [ЛР7 — Витрина данных на Scala Spark](./LAB7_DATAMART_SCALA_BEGINNER.md)
- [ЛР8 — Миграция в Kubernetes](./LAB8_KUBERNETES_MIGRATION_BEGINNER.md)

И отдельная теоретическая шпаргалка:

- [Теоретическая справка (ML + Spark + Redis + Kafka + Vault + Mongo + Kubernetes)](./THEORY_REFERENCE_BEGINNER.md)

---

## Как читать эти файлы, если ты «с нуля»

Рекомендую такой порядок:

1. Сначала открыть `THEORY_REFERENCE_BEGINNER.md`.
2. Затем читать лабы по порядку от 1 до 8.
3. В каждой лабе сначала читать блоки:
   - «Зачем эта лаба»
   - «Что было до / что стало после»
   - «Пошагово как это разрабатывалось»
4. Потом перейти к разделу «Разбор кода».
5. В конце пройти «Чеклист защиты» и «Типовые ошибки».

---

## В каких репозиториях находятся коды

- **ЛР1–ЛР4**: `classic_developer_ml_cicle`
- **ЛР5–ЛР8**: `itmo_spark`

В этих гайдах это уже учтено: у каждого примера указано, из какого контура он взят.
