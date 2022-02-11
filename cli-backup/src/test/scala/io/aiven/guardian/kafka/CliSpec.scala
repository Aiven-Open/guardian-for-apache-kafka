package io.aiven.guardian.kafka

import io.aiven.guardian.kafka.backup.configs.ChronoUnitSlice
import io.aiven.guardian.kafka.backup.configs.{Backup => BackupConfig}
import io.aiven.guardian.kafka.configs.{KafkaCluster => KafkaClusterConfig}
import org.scalatest.matchers.must.Matchers
import org.scalatest.propspec.AnyPropSpec

import scala.annotation.nowarn

import java.time.temporal.ChronoUnit

@nowarn("msg=method main in class CommandApp is deprecated")
class CliSpec extends AnyPropSpec with Matchers {

  property("Command line args are properly passed into application") {
    val groupId         = "my-consumer-group"
    val topic           = "topic"
    val bootstrapServer = "localhost:9092"
    val dataBucket      = "backup-bucket"

    val args = List(
      "--storage",
      "s3",
      "--kafka-topics",
      topic,
      "--kafka-bootstrap-servers",
      bootstrapServer,
      "--s3-data-bucket",
      dataBucket,
      "--kafka-group-id",
      groupId,
      "--chrono-unit-slice",
      "hours"
    )

    try backup.Main.main(args.toArray)
    catch {
      case _: Throwable =>
    }
    backup.Main.initializedApp.get() match {
      case Some(s3App: backup.S3App) =>
        s3App.backupConfig mustEqual BackupConfig(groupId, ChronoUnitSlice(ChronoUnit.HOURS))
        s3App.kafkaClusterConfig mustEqual KafkaClusterConfig(Set(topic))
        s3App.kafkaClient.consumerSettings.getProperty("bootstrap.servers") mustEqual bootstrapServer
        s3App.s3Config.dataBucket mustEqual dataBucket
      case _ =>
        fail("Expected an App to be initialized")
    }
  }

}
