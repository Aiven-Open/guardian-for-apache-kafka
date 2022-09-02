package io.aiven.guardian.kafka.backup

import akka.Done
import akka.kafka.scaladsl.Consumer
import akka.stream.Materializer

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** A wrapper that is designed to make it easier to cleanly shutdown resources in tests
  */
class BackupClientControlWrapper[T <: KafkaClient](backupClient: BackupClientInterface[T])(implicit
    materializer: Materializer,
    ec: ExecutionContext
) {

  private var control: Consumer.DrainingControl[Done] = _

  def run(): Unit =
    control = backupClient.backup.run()

  def shutdown(): Future[Done] = control.drainAndShutdown()
}
