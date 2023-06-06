package io.aiven.guardian.kafka.backup

import org.apache.pekko

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import pekko.Done
import pekko.kafka.scaladsl.Consumer
import pekko.stream.Materializer

/** A wrapper that is designed to make it easier to cleanly shutdown resources in tests
  */
class BackupClientControlWrapper[T <: KafkaConsumer](backupClient: BackupClientInterface[T])(implicit
    materializer: Materializer,
    ec: ExecutionContext
) {

  private var control: Consumer.DrainingControl[Done] = _

  def run(): Unit =
    control = backupClient.backup.run()

  @SuppressWarnings(Array("DisableSyntax.null"))
  def shutdown(): Future[Done] =
    if (control != null)
      control.drainAndShutdown()
    else
      Future.successful(Done)
}
