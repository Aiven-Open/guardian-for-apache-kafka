package io.aiven.guardian.kafka.backup.s3

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.s3.S3Headers
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.SuccessfulUploadPart
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import io.aiven.guardian.kafka.backup.KafkaConsumerInterface
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.s3.configs.{S3 => S3Config}

import scala.collection.immutable
import scala.concurrent.Future

import java.util.concurrent.ConcurrentLinkedQueue

class BackupClientChunkState[T <: KafkaConsumerInterface](maybeS3Settings: Option[S3Settings])(implicit
    override val kafkaClientInterface: T,
    override val backupConfig: Backup,
    override val system: ActorSystem,
    s3Config: S3Config,
    s3Headers: S3Headers
) extends BackupClient[T](maybeS3Settings) {
  val processedChunks: ConcurrentLinkedQueue[SuccessfulUploadPart] = new ConcurrentLinkedQueue[SuccessfulUploadPart]()

  override def successSink
      : Sink[(SuccessfulUploadPart, immutable.Iterable[kafkaClientInterface.CursorContext]), Future[Done]] =
    Flow[(SuccessfulUploadPart, immutable.Iterable[kafkaClientInterface.CursorContext])]
      .alsoTo(Sink.foreach { case (part, _) =>
        processedChunks.add(part)
      })
      .to(super.successSink)
      .mapMaterializedValue(_ => Future.successful(Done))
}
