package io.aiven.guardian.kafka.backup

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorAttributes
import akka.stream.Supervision
import com.typesafe.scalalogging.StrictLogging
import io.aiven.guardian.kafka.backup.BackupClientInterface
import io.aiven.guardian.kafka.backup.KafkaClient
import io.aiven.guardian.kafka.backup.KafkaClientInterface

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait App[T <: KafkaClientInterface] extends StrictLogging {
  implicit val kafkaClient: T
  implicit val backupClient: BackupClientInterface[KafkaClient]
  implicit val actorSystem: ActorSystem
  implicit val executionContext: ExecutionContext

  def run(): Consumer.Control = {
    val decider: Supervision.Decider = { e =>
      logger.error("Unhandled exception in stream", e)
      Supervision.Stop
    }

    backupClient.backup.withAttributes(ActorAttributes.supervisionStrategy(decider)).run()
  }

  def shutdown(control: Consumer.Control): Future[Done] = {
    logger.warn("Shutdown of Guardian detected")
    // Ideally we should be using drainAndShutdown however this isn't possible due to
    // https://github.com/aiven/guardian-for-apache-kafka/issues/80
    control.stop().flatMap(_ => control.shutdown())
  }
}
