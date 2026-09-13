package itmo.lab7.service

import itmo.lab7.config.DataMartConfig
import itmo.lab7.model.{FeatureRecord, ModelPredictionRecord}
import itmo.lab7.mongo.{ModelMetrics, MongoGateway}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.{Files, Path, Paths}
import java.util.Comparator

final case class PrepareResult(runId: String, sourceRows: Long, preparedRows: Long, bootstrappedRows: Long)
final case class PublishResult(runId: String, loadedRows: Long, bestK: Int, bestSilhouette: Double)

class DataMartService(config: DataMartConfig) {
  private val mongoGateway = new MongoGateway(config.mongo)

  private def createSparkSession(appNameSuffix: String): SparkSession = {
    SparkSession.builder()
      .appName(s"${config.spark.appName}-${appNameSuffix}")
      .master(config.spark.master)
      .config("spark.sql.shuffle.partitions", config.spark.shufflePartitions.toString)
      .getOrCreate()
  }

  private def applyPreprocessingBounds(df: DataFrame): DataFrame = {
    df
      .filter(col("energy_kcal_100g").between(config.preprocessing.energyMin, config.preprocessing.energyMax))
      .filter(col("fat_100g").between(config.preprocessing.fatMin, config.preprocessing.fatMax))
      .filter(col("carbohydrates_100g").between(config.preprocessing.carbohydratesMin, config.preprocessing.carbohydratesMax))
      .filter(col("sugars_100g").between(config.preprocessing.sugarsMin, config.preprocessing.sugarsMax))
      .filter(col("proteins_100g").between(config.preprocessing.proteinsMin, config.preprocessing.proteinsMax))
      .filter(col("salt_100g").between(config.preprocessing.saltMin, config.preprocessing.saltMax))
  }

  private def ensureParentDir(path: Path): Unit = {
    val parent = path.toAbsolutePath.getParent
    if (parent != null) {
      Files.createDirectories(parent)
    }
  }

  private def deleteDirectory(path: Path): Unit = {
    if (!Files.exists(path)) {
      return
    }
    Files
      .walk(path)
      .sorted(Comparator.reverseOrder())
      .forEach { filePath =>
        Files.delete(filePath)
      }
  }

  private def readMetrics(path: Path): ModelMetrics = {
    val json = ujson.read(Files.readString(path))
    val bestK = json("best_k").num.toInt
    val bestSilhouette = json("best_silhouette").num
    ModelMetrics(bestK, bestSilhouette)
  }

  def prepareModelInput(
      runId: String,
      bootstrapIfEmpty: Boolean,
      bootstrapFile: Option[String],
      bootstrapLines: Int
  ): PrepareResult = {
    mongoGateway.updateRunState(runId, "mart_started")

    val sourceCountBefore = mongoGateway.sourceCount()
    val bootstrappedRows =
      if (sourceCountBefore == 0 && bootstrapIfEmpty) {
        val filePath = bootstrapFile.getOrElse {
          throw new IllegalArgumentException(
            "Bootstrap file is required when --bootstrap-if-empty is enabled and source is empty"
          )
        }
        val inserted = mongoGateway.bootstrapSourceFromJsonl(Paths.get(filePath), bootstrapLines)
        mongoGateway.updateRunState(runId, "mart_bootstrapped", Map("bootstrapped_rows" -> inserted))
        inserted.toLong
      } else {
        0L
      }

    val sourceRecords = mongoGateway.fetchSourceRecords()
    if (sourceRecords.isEmpty) {
      throw new IllegalStateException("Source collection is empty. Cannot prepare data mart dataset.")
    }

    val spark = createSparkSession("prepare")
    try {
      val rows = sourceRecords.map(FeatureRecord.toRow)
      val rowRdd = spark.sparkContext.parallelize(rows)
      val sourceDf = spark.createDataFrame(rowRdd, FeatureRecord.schema)
      val preparedDf = applyPreprocessingBounds(sourceDf)

      val preparedPath = Paths.get(config.paths.preparedParquet)
      if (Files.exists(preparedPath)) {
        deleteDirectory(preparedPath)
      }
      ensureParentDir(preparedPath)
      preparedDf.write.mode("overwrite").parquet(config.paths.preparedParquet)

      val preparedRows = preparedDf.collect().toSeq.map(FeatureRecord.fromRow)
      mongoGateway.replaceMartFeatures(runId, preparedRows)

      mongoGateway.updateRunState(
        runId,
        "mart_prepared",
        Map(
          "source_rows" -> sourceRecords.size,
          "prepared_rows" -> preparedRows.size,
          "prepared_parquet" -> config.paths.preparedParquet
        )
      )

      PrepareResult(
        runId = runId,
        sourceRows = sourceRecords.size.toLong,
        preparedRows = preparedRows.size.toLong,
        bootstrappedRows = bootstrappedRows
      )
    } finally {
      spark.stop()
    }
  }

  def publishModelResults(runId: String): PublishResult = {
    val metricsPath = Paths.get(config.paths.modelMetricsJson)
    val predictionsPath = Paths.get(config.paths.modelPredictionsParquet)

    if (!Files.exists(metricsPath)) {
      throw new IllegalArgumentException(s"Metrics file not found: ${metricsPath.toAbsolutePath}")
    }
    if (!Files.exists(predictionsPath)) {
      throw new IllegalArgumentException(s"Predictions parquet not found: ${predictionsPath.toAbsolutePath}")
    }

    val metrics = readMetrics(metricsPath)

    val spark = createSparkSession("publish")
    try {
      val predictionsDf = spark.read.parquet(config.paths.modelPredictionsParquet)
      val rows = predictionsDf.collect().toSeq.map(ModelPredictionRecord.fromRow)
      val inserted = mongoGateway.appendModelResults(runId, metrics, rows)

      mongoGateway.updateRunState(
        runId,
        "mart_completed",
        Map(
          "loaded_rows" -> inserted,
          "best_k" -> metrics.bestK,
          "best_silhouette" -> metrics.bestSilhouette
        )
      )

      PublishResult(
        runId = runId,
        loadedRows = inserted.toLong,
        bestK = metrics.bestK,
        bestSilhouette = metrics.bestSilhouette
      )
    } finally {
      spark.stop()
    }
  }
}
