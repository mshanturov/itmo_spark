package itmo.lab7.mongo

import itmo.lab7.config.MongoConfig
import itmo.lab7.model.{FeatureRecord, ModelPredictionRecord}
import org.bson.Document
import org.bson.conversions.Bson

import java.nio.file.{Files, Path}
import java.time.Instant
import java.util
import com.mongodb.client.model.Filters
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import com.mongodb.client.{MongoClient, MongoClients, MongoCollection}
import scala.jdk.CollectionConverters._
import scala.io.Source
import scala.util.Using
import ujson.Value

final case class ModelMetrics(bestK: Int, bestSilhouette: Double)

class MongoGateway(config: MongoConfig) {
  private def withClient[A](fn: MongoClient => A): A = {
    val client = MongoClients.create(config.uri)
    try fn(client)
    finally client.close()
  }

  private def nowIso: String = Instant.now().toString

  private def sourceCollection(client: MongoClient): MongoCollection[Document] =
    client.getDatabase(config.database).getCollection(config.sourceCollection)

  private def martCollection(client: MongoClient): MongoCollection[Document] =
    client.getDatabase(config.database).getCollection(config.martCollection)

  private def resultsCollection(client: MongoClient): MongoCollection[Document] =
    client.getDatabase(config.database).getCollection(config.resultsCollection)

  private def runsCollection(client: MongoClient): MongoCollection[Document] =
    client.getDatabase(config.database).getCollection(config.runsCollection)

  def updateRunState(runId: String, state: String, payload: Map[String, Any] = Map.empty): Unit = {
    withClient { client =>
      val updates = new util.ArrayList[Bson]()
      updates.add(Updates.set("state", state))
      updates.add(Updates.set("updated_at", nowIso))
      payload.foreach { case (key, value) => updates.add(Updates.set(key, value)) }

      val update = Updates.combine(updates)
      runsCollection(client).updateOne(
        Filters.eq("run_id", runId),
        Updates.combine(update, Updates.setOnInsert("run_id", runId), Updates.setOnInsert("created_at", nowIso)),
        new UpdateOptions().upsert(true)
      )
    }
  }

  def sourceCount(): Long = {
    withClient { client =>
      sourceCollection(client).countDocuments()
    }
  }

  def bootstrapSourceFromJsonl(path: Path, maxLines: Int): Int = {
    if (!Files.exists(path)) {
      throw new IllegalArgumentException(s"Bootstrap file not found: ${path.toAbsolutePath}")
    }

    val docs = Using.resource(Source.fromFile(path.toFile, "UTF-8")) { source =>
      source.getLines().take(maxLines).flatMap(parseSourceDocument).toSeq
    }

    if (docs.isEmpty) {
      0
    } else {
      withClient { client =>
        sourceCollection(client).insertMany(docs.asJava)
      }
      docs.size
    }
  }

  private def parseSourceDocument(line: String): Option[Document] = {
    val json = ujson.read(line)
    val nutriments = json.obj.get("nutriments")
    nutriments.flatMap { nut =>
      extractFeatureRecord(json, nut).map { record =>
        featureRecordToSourceDoc(record)
      }
    }
  }

  private def extractFeatureRecord(root: Value, nutriments: Value): Option[FeatureRecord] = {
    def toDouble(key: String): Option[Double] = {
      nutriments.obj.get(key).flatMap { value =>
        value match {
          case n: ujson.Num => Some(n.value)
          case s: ujson.Str => s.value.toDoubleOption
          case _            => None
        }
      }
    }

    for {
      energy <- toDouble("energy-kcal_100g")
      fat <- toDouble("fat_100g")
      carbs <- toDouble("carbohydrates_100g")
      sugars <- toDouble("sugars_100g")
      proteins <- toDouble("proteins_100g")
      salt <- toDouble("salt_100g")
    } yield {
      FeatureRecord(
        code = root.obj.get("code").collect { case s: ujson.Str => s.value }.getOrElse(""),
        productName = root.obj.get("product_name").collect { case s: ujson.Str => s.value }.getOrElse(""),
        energyKcal100g = energy,
        fat100g = fat,
        carbohydrates100g = carbs,
        sugars100g = sugars,
        proteins100g = proteins,
        salt100g = salt
      )
    }
  }

  private def featureRecordToSourceDoc(record: FeatureRecord): Document = {
    new Document()
      .append("code", record.code)
      .append("product_name", record.productName)
      .append(
        "nutriments",
        new Document()
          .append("energy-kcal_100g", record.energyKcal100g)
          .append("fat_100g", record.fat100g)
          .append("carbohydrates_100g", record.carbohydrates100g)
          .append("sugars_100g", record.sugars100g)
          .append("proteins_100g", record.proteins100g)
          .append("salt_100g", record.salt100g)
      )
      .append("source", "openfoodfacts")
  }

  def fetchSourceRecords(): Seq[FeatureRecord] = {
    withClient { client =>
      sourceCollection(client)
        .find()
        .iterator()
        .asScala
        .flatMap(documentToFeatureRecord)
        .toSeq
    }
  }

  private def documentToFeatureRecord(document: Document): Option[FeatureRecord] = {
    val nutriments = Option(document.get("nutriments", classOf[Document]))

    def toDouble(value: AnyRef): Option[Double] = value match {
      case d: java.lang.Double  => Some(d.doubleValue())
      case f: java.lang.Float   => Some(f.toDouble)
      case i: java.lang.Integer => Some(i.toDouble)
      case l: java.lang.Long    => Some(l.toDouble)
      case s: String            => s.toDoubleOption
      case _                    => None
    }

    nutriments.flatMap { nut =>
      for {
        energy <- toDouble(nut.get("energy-kcal_100g"))
        fat <- toDouble(nut.get("fat_100g"))
        carbs <- toDouble(nut.get("carbohydrates_100g"))
        sugars <- toDouble(nut.get("sugars_100g"))
        proteins <- toDouble(nut.get("proteins_100g"))
        salt <- toDouble(nut.get("salt_100g"))
      } yield {
        FeatureRecord(
          code = Option(document.getString("code")).getOrElse(""),
          productName = Option(document.getString("product_name")).getOrElse(""),
          energyKcal100g = energy,
          fat100g = fat,
          carbohydrates100g = carbs,
          sugars100g = sugars,
          proteins100g = proteins,
          salt100g = salt
        )
      }
    }
  }

  def replaceMartFeatures(runId: String, records: Seq[FeatureRecord]): Int = {
    withClient { client =>
      val collection = martCollection(client)
      collection.deleteMany(Filters.eq("run_id", runId))

      if (records.nonEmpty) {
        val docs = records.map { record =>
          new Document()
            .append("run_id", runId)
            .append("prepared_at", nowIso)
            .append("code", record.code)
            .append("product_name", record.productName)
            .append(
              "features",
              new Document()
                .append("energy_kcal_100g", record.energyKcal100g)
                .append("fat_100g", record.fat100g)
                .append("carbohydrates_100g", record.carbohydrates100g)
                .append("sugars_100g", record.sugars100g)
                .append("proteins_100g", record.proteins100g)
                .append("salt_100g", record.salt100g)
            )
        }
        collection.insertMany(docs.asJava)
      }
    }
    records.size
  }

  def appendModelResults(runId: String, metrics: ModelMetrics, rows: Seq[ModelPredictionRecord]): Int = {
    withClient { client =>
      if (rows.nonEmpty) {
        val docs = rows.map { row =>
          new Document()
            .append("run_id", runId)
            .append("processed_at", nowIso)
            .append("model", "kmeans")
            .append("best_k", metrics.bestK)
            .append("best_silhouette", metrics.bestSilhouette)
            .append("code", row.code)
            .append("product_name", row.productName)
            .append(
              "features",
              new Document()
                .append("energy_kcal_100g", row.energyKcal100g)
                .append("fat_100g", row.fat100g)
                .append("carbohydrates_100g", row.carbohydrates100g)
                .append("sugars_100g", row.sugars100g)
                .append("proteins_100g", row.proteins100g)
                .append("salt_100g", row.salt100g)
            )
            .append("cluster_id", row.clusterId)
        }

        resultsCollection(client).insertMany(docs.asJava)
      }
    }
    rows.size
  }
}
