package io.aiven.guardian.kafka.backup

import com.typesafe.scalalogging.LazyLogging
import io.aiven.guardian.kafka.backup.BackupClientInterface
import io.aiven.guardian.kafka.backup.KafkaConsumer
import io.aiven.guardian.kafka.backup.KafkaConsumerInterface
import org.apache.pekko

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import pekko.Done
import pekko.actor.ActorSystem
import pekko.kafka.scaladsl.Consumer
import pekko.stream.ActorAttributes
import pekko.stream.Supervision

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
