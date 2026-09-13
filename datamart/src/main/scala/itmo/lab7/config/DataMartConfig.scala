package itmo.lab7.config

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File

final case class MongoConfig(
    uri: String,
    database: String,
    sourceCollection: String,
    martCollection: String,
    resultsCollection: String,
    runsCollection: String
)

final case class PathsConfig(
    preparedParquet: String,
    modelPredictionsParquet: String,
    modelMetricsJson: String
)

final case class PreprocessingConfig(
    energyMin: Double,
    energyMax: Double,
    fatMin: Double,
    fatMax: Double,
    carbohydratesMin: Double,
    carbohydratesMax: Double,
    sugarsMin: Double,
    sugarsMax: Double,
    proteinsMin: Double,
    proteinsMax: Double,
    saltMin: Double,
    saltMax: Double
)

final case class SparkConfig(
    master: String,
    appName: String,
    shufflePartitions: Int
)

final case class DataMartConfig(
    mongo: MongoConfig,
    paths: PathsConfig,
    preprocessing: PreprocessingConfig,
    spark: SparkConfig
)

object DataMartConfig {
  def load(path: String): DataMartConfig = {
    val config = ConfigFactory.parseFile(new File(path)).resolve()
    fromConfig(config)
  }

  private def fromConfig(config: Config): DataMartConfig = {
    DataMartConfig(
      mongo = MongoConfig(
        uri = config.getString("mongodb.uri"),
        database = config.getString("mongodb.database"),
        sourceCollection = config.getString("mongodb.sourceCollection"),
        martCollection = config.getString("mongodb.martCollection"),
        resultsCollection = config.getString("mongodb.resultsCollection"),
        runsCollection = config.getString("mongodb.runsCollection")
      ),
      paths = PathsConfig(
        preparedParquet = config.getString("paths.preparedParquet"),
        modelPredictionsParquet = config.getString("paths.modelPredictionsParquet"),
        modelMetricsJson = config.getString("paths.modelMetricsJson")
      ),
      preprocessing = PreprocessingConfig(
        energyMin = config.getDouble("preprocessing.energyMin"),
        energyMax = config.getDouble("preprocessing.energyMax"),
        fatMin = config.getDouble("preprocessing.fatMin"),
        fatMax = config.getDouble("preprocessing.fatMax"),
        carbohydratesMin = config.getDouble("preprocessing.carbohydratesMin"),
        carbohydratesMax = config.getDouble("preprocessing.carbohydratesMax"),
        sugarsMin = config.getDouble("preprocessing.sugarsMin"),
        sugarsMax = config.getDouble("preprocessing.sugarsMax"),
        proteinsMin = config.getDouble("preprocessing.proteinsMin"),
        proteinsMax = config.getDouble("preprocessing.proteinsMax"),
        saltMin = config.getDouble("preprocessing.saltMin"),
        saltMax = config.getDouble("preprocessing.saltMax")
      ),
      spark = SparkConfig(
        master = config.getString("spark.master"),
        appName = config.getString("spark.appName"),
        shufflePartitions = config.getInt("spark.shufflePartitions")
      )
    )
  }
}
