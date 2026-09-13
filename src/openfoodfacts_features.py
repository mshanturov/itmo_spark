from __future__ import annotations

from pyspark.sql import types as T

FEATURE_COLUMNS = [
    "energy_kcal_100g",
    "fat_100g",
    "carbohydrates_100g",
    "sugars_100g",
    "proteins_100g",
    "salt_100g",
]

FEATURE_BOUNDS = {
    "energy_kcal_100g": (0, 1200),
    "fat_100g": (0, 100),
    "carbohydrates_100g": (0, 100),
    "sugars_100g": (0, 100),
    "proteins_100g": (0, 100),
    "salt_100g": (0, 100),
}

FEATURE_RATIONALE = {
    "energy_kcal_100g": "Базовый показатель калорийности, отделяет низко- и высокоэнергетические продукты.",
    "fat_100g": "Отражает долю жиров и помогает отделять, например, молочные/масляные группы.",
    "carbohydrates_100g": "Ключевой макроэлемент для различения зерновых, сладостей и других категорий.",
    "sugars_100g": "Частный случай углеводов, критичен для выделения сладких продуктов.",
    "proteins_100g": "Позволяет выделить белковые группы (мясо, бобовые, протеиновые продукты).",
    "salt_100g": "Добавляет различимость солёных и ультраобработанных продуктов.",
}

OPENFOODFACTS_SCHEMA = T.StructType(
    [
        T.StructField("code", T.StringType(), True),
        T.StructField("product_name", T.StringType(), True),
        T.StructField(
            "nutriments",
            T.StructType(
                [
                    T.StructField("energy-kcal_100g", T.DoubleType(), True),
                    T.StructField("fat_100g", T.DoubleType(), True),
                    T.StructField("carbohydrates_100g", T.DoubleType(), True),
                    T.StructField("sugars_100g", T.DoubleType(), True),
                    T.StructField("proteins_100g", T.DoubleType(), True),
                    T.StructField("salt_100g", T.DoubleType(), True),
                ]
            ),
            True,
        ),
    ]
)
