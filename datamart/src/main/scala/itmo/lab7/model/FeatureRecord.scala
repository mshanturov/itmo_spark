package itmo.lab7.model

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, RowFactory}

final case class FeatureRecord(
    code: String,
    productName: String,
    energyKcal100g: Double,
    fat100g: Double,
    carbohydrates100g: Double,
    sugars100g: Double,
    proteins100g: Double,
    salt100g: Double
)

object FeatureRecord {
  val schema: StructType = StructType(
    Seq(
      StructField("code", StringType, nullable = true),
      StructField("product_name", StringType, nullable = true),
      StructField("energy_kcal_100g", DoubleType, nullable = false),
      StructField("fat_100g", DoubleType, nullable = false),
      StructField("carbohydrates_100g", DoubleType, nullable = false),
      StructField("sugars_100g", DoubleType, nullable = false),
      StructField("proteins_100g", DoubleType, nullable = false),
      StructField("salt_100g", DoubleType, nullable = false)
    )
  )

  def toRow(record: FeatureRecord): Row = {
    RowFactory.create(
      record.code,
      record.productName,
      java.lang.Double.valueOf(record.energyKcal100g),
      java.lang.Double.valueOf(record.fat100g),
      java.lang.Double.valueOf(record.carbohydrates100g),
      java.lang.Double.valueOf(record.sugars100g),
      java.lang.Double.valueOf(record.proteins100g),
      java.lang.Double.valueOf(record.salt100g)
    )
  }

  def fromRow(row: Row): FeatureRecord = {
    FeatureRecord(
      code = Option(row.getAs[String]("code")).getOrElse(""),
      productName = Option(row.getAs[String]("product_name")).getOrElse(""),
      energyKcal100g = row.getAs[Double]("energy_kcal_100g"),
      fat100g = row.getAs[Double]("fat_100g"),
      carbohydrates100g = row.getAs[Double]("carbohydrates_100g"),
      sugars100g = row.getAs[Double]("sugars_100g"),
      proteins100g = row.getAs[Double]("proteins_100g"),
      salt100g = row.getAs[Double]("salt_100g")
    )
  }
}

final case class ModelPredictionRecord(
    code: String,
    productName: String,
    energyKcal100g: Double,
    fat100g: Double,
    carbohydrates100g: Double,
    sugars100g: Double,
    proteins100g: Double,
    salt100g: Double,
    clusterId: Int
)

object ModelPredictionRecord {
  def fromRow(row: Row): ModelPredictionRecord = {
    ModelPredictionRecord(
      code = Option(row.getAs[String]("code")).getOrElse(""),
      productName = Option(row.getAs[String]("product_name")).getOrElse(""),
      energyKcal100g = row.getAs[Double]("energy_kcal_100g"),
      fat100g = row.getAs[Double]("fat_100g"),
      carbohydrates100g = row.getAs[Double]("carbohydrates_100g"),
      sugars100g = row.getAs[Double]("sugars_100g"),
      proteins100g = row.getAs[Double]("proteins_100g"),
      salt100g = row.getAs[Double]("salt_100g"),
      clusterId = row.getAs[Int]("prediction")
    )
  }
}
