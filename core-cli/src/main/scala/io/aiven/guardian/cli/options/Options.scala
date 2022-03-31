package io.aiven.guardian.cli.options

import cats.data.NonEmptyList
import cats.implicits._
import com.monovore.decline.Opts
import com.typesafe.config.ConfigException.Missing
import com.typesafe.config.ConfigFactory
import io.aiven.guardian.cli.arguments.StorageOpt
import io.aiven.guardian.kafka.configs.KafkaCluster
import pureconfig.error.ConfigReaderException

import java.nio.file.Path

trait Options {
  val storageOpt: Opts[StorageOpt] =
    Opts.option[StorageOpt]("storage", help = "Which type of storage to persist kafka topics")

  val dataBucketOpt: Opts[Option[String]] =
    Opts.option[String]("s3-data-bucket", help = "S3 Bucket for storage of main backup data").orNone

  val topicsOpt: Opts[Option[NonEmptyList[String]]] =
    Opts.options[String]("kafka-topics", help = "Kafka topics to operate on").orNone

  val bootstrapServersOpt: Opts[Option[NonEmptyList[String]]] =
    Opts.options[String]("kafka-bootstrap-servers", help = "Kafka bootstrap servers").orNone

  val logbackFileOpt: Opts[Option[Path]] =
    Opts.option[Path]("logback-file", help = "Specify logback.xml configuration to override default").orNone

  def optionalPureConfigValue[T](value: () => T): Option[T] =
    try Some(value())
    catch {
      case _: ConfigReaderException[_] =>
        None
    }

  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.null"
    )
  )
  def checkConfigKeyIsDefined(path: String): Boolean =
    try ConfigFactory.load().getAnyRef(path) != null
    catch {
      case _: Missing => false
    }

  val kafkaClusterOpt: Opts[KafkaCluster] = topicsOpt.mapValidated { topics =>
    import io.aiven.guardian.kafka.{Config => KafkaConfig}
    topics match {
      case Some(value) =>
        KafkaCluster(value.toList.toSet).validNel
      case None if KafkaConfig.kafkaClusterConfig.topics.nonEmpty => KafkaConfig.kafkaClusterConfig.validNel
      case _ =>
        "kafka-topics is a mandatory value that needs to be configured".invalidNel
    }
  }

}

object Options extends Options
