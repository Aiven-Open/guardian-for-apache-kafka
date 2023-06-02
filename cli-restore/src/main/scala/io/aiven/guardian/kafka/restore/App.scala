package io.aiven.guardian.kafka.restore

import com.typesafe.scalalogging.LazyLogging
import io.aiven.guardian.kafka.restore.KafkaProducer
import io.aiven.guardian.kafka.restore.s3.RestoreClient
import org.apache.pekko

import scala.concurrent.Future

import pekko.Done
import pekko.actor.ActorSystem
import pekko.stream.ActorAttributes
import pekko.stream.KillSwitch
import pekko.stream.Supervision
import pekko.stream.UniqueKillSwitch

trait App extends LazyLogging {
  implicit val kafkaProducer: KafkaProducer
  implicit val restoreClient: RestoreClient[KafkaProducer]
  implicit val actorSystem: ActorSystem

  val decider: Supervision.Decider = { e =>
    logger.error("Unhandled exception in stream", e)
    Supervision.Stop
  }

  def run(): (UniqueKillSwitch, Future[Done]) =
    restoreClient.restore.withAttributes(ActorAttributes.supervisionStrategy(decider)).run()

  def shutdown(killSwitch: KillSwitch): Unit = {
    logger.info("Shutdown of Guardian detected")
    killSwitch.shutdown()
    logger.info("Guardian shut down")
  }
}
