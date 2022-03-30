package io.aiven.guardian.kafka.restore

import io.aiven.guardian.kafka.configs.{KafkaCluster => KafkaClusterConfig}
import io.aiven.guardian.kafka.restore.configs.{Restore => RestoreConfig}
import org.apache.kafka.clients.producer.ProducerConfig
import org.scalatest.matchers.must.Matchers
import org.scalatest.propspec.AnyPropSpec

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

import java.time.Instant
import java.time.ZoneId

@nowarn("msg=method main in class CommandApp is deprecated")
class CliSpec extends AnyPropSpec with Matchers {

  property("Command line args are properly passed into application") {
    val bootstrapServer  = "localhost:9092"
    val fromWhen         = Instant.ofEpochMilli(0).atZone(ZoneId.of("UTC")).toOffsetDateTime
    val topic1           = "topic-1"
    val topic2           = "topic-2"
    val restoreTopicOne  = s"restore-$topic1"
    val restoreTopicTwo  = s"restore-$topic2"
    val overrideTopicOne = s"$topic1:$restoreTopicOne"
    val overrideTopicTwo = s"$topic2:$restoreTopicTwo"
    val dataBucket       = "backup-bucket"

    val args = List(
      "--storage",
      "s3",
      "--kafka-topics",
      topic1,
      "--kafka-topics",
      topic2,
      "--kafka-bootstrap-servers",
      bootstrapServer,
      "--s3-data-bucket",
      dataBucket,
      "--from-when",
      fromWhen.toString,
      "--override-topics",
      overrideTopicOne,
      "--override-topics",
      overrideTopicTwo,
      "--single-message-per-kafka-request"
    )

    try Main.main(args.toArray)
    catch {
      case _: Throwable =>
    }
    Main.initializedApp.get() match {
      case Some(s3App: S3App) =>
        s3App.restoreConfig mustEqual RestoreConfig(Some(fromWhen),
                                                    Some(
                                                      Map(
                                                        topic1 -> restoreTopicOne,
                                                        topic2 -> restoreTopicTwo
                                                      )
                                                    )
        )
        s3App.kafkaClusterConfig mustEqual KafkaClusterConfig(Set(topic1, topic2))
        s3App.kafkaProducer.producerSettings.getProperties.get("bootstrap.servers") mustEqual bootstrapServer
        s3App.s3Config.dataBucket mustEqual dataBucket
        (Map(
          ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG             -> true.toString,
          ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> 1.toString,
          ProducerConfig.BATCH_SIZE_CONFIG                     -> 0.toString
        ): Map[String, AnyRef]).toSet
          .subsetOf(s3App.kafkaProducer.producerSettings.getProperties.asScala.toMap.toSet) mustEqual true
        s3App.kafkaProducer.producerSettings.parallelism mustEqual 1
      case _ =>
        fail("Expected an App to be initialized")
    }
  }
}
