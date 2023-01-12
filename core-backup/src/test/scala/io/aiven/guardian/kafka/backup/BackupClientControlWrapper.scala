package io.aiven.guardian.kafka.backup

import akka.Done
import akka.kafka.scaladsl.Consumer
import akka.stream.Materializer

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

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
