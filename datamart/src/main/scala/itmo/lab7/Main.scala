package itmo.lab7

import itmo.lab7.config.DataMartConfig
import itmo.lab7.service.DataMartService

object Main {
  sealed trait Action
  case object Prepare extends Action
  case object Publish extends Action

  final case class CliOptions(
      action: Action,
      configPath: String,
      runId: String,
      bootstrapIfEmpty: Boolean,
      bootstrapFile: Option[String],
      bootstrapLines: Int
  )

  private def parseArgs(args: Array[String]): CliOptions = {
    if (args.isEmpty) {
      throw new IllegalArgumentException(
        "Usage: prepare|publish --config <path> --run-id <id> [--bootstrap-if-empty --bootstrap-file <path> --bootstrap-lines <n>]"
      )
    }

    val action = args.head match {
      case "prepare" => Prepare
      case "publish" => Publish
      case value      => throw new IllegalArgumentException(s"Unknown action: $value")
    }

    var configPath = "conf/datamart-local.conf"
    var runId = ""
    var bootstrapIfEmpty = false
    var bootstrapFile: Option[String] = None
    var bootstrapLines = 15000

    var index = 1
    while (index < args.length) {
      args(index) match {
        case "--config" =>
          index += 1
          configPath = args(index)
        case "--run-id" =>
          index += 1
          runId = args(index)
        case "--bootstrap-if-empty" =>
          bootstrapIfEmpty = true
        case "--bootstrap-file" =>
          index += 1
          bootstrapFile = Some(args(index))
        case "--bootstrap-lines" =>
          index += 1
          bootstrapLines = args(index).toInt
        case unknown =>
          throw new IllegalArgumentException(s"Unknown option: $unknown")
      }
      index += 1
    }

    if (runId.trim.isEmpty) {
      throw new IllegalArgumentException("--run-id is required")
    }

    CliOptions(
      action = action,
      configPath = configPath,
      runId = runId,
      bootstrapIfEmpty = bootstrapIfEmpty,
      bootstrapFile = bootstrapFile,
      bootstrapLines = bootstrapLines
    )
  }

  def main(args: Array[String]): Unit = {
    val options = parseArgs(args)
    val config = DataMartConfig.load(options.configPath)
    val service = new DataMartService(config)

    options.action match {
      case Prepare =>
        val result = service.prepareModelInput(
          runId = options.runId,
          bootstrapIfEmpty = options.bootstrapIfEmpty,
          bootstrapFile = options.bootstrapFile,
          bootstrapLines = options.bootstrapLines
        )
        println(
          s"Prepared data mart for run ${result.runId}. source_rows=${result.sourceRows}, prepared_rows=${result.preparedRows}, bootstrapped_rows=${result.bootstrappedRows}"
        )
      case Publish =>
        val result = service.publishModelResults(options.runId)
        println(
          s"Published model results for run ${result.runId}. loaded_rows=${result.loadedRows}, best_k=${result.bestK}, best_silhouette=${result.bestSilhouette}"
        )
    }
  }
}
