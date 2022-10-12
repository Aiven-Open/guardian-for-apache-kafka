package io.aiven.guardian.kafka.backup

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorAttributes
import akka.stream.Supervision
import com.typesafe.scalalogging.LazyLogging
import io.aiven.guardian.kafka.backup.BackupClientInterface
import io.aiven.guardian.kafka.backup.KafkaConsumer
import io.aiven.guardian.kafka.backup.KafkaConsumerInterface

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait App[T <: KafkaConsumerInterface] extends LazyLogging {
  implicit val kafkaClient: T
  implicit val backupClient: BackupClientInterface[KafkaConsumer]
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
    logger.info("Shutdown of Guardian detected")
    val future = control.shutdown()
    future.onComplete(_ => logger.info("Guardian shut down"))
    future
  }
}
