package io.aiven.guardian.kafka.backup.s3

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.s3.FailedUploadPart
import akka.stream.alpakka.s3.MultipartUploadResult
import akka.stream.alpakka.s3.Part
import akka.stream.alpakka.s3.S3Attributes
import akka.stream.alpakka.s3.S3Headers
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.SuccessfulUploadPart
import akka.stream.alpakka.s3.UploadPartResponse
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl._
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging
import io.aiven.guardian.kafka.KafkaClientInterface
import io.aiven.guardian.kafka.backup.BackupClientInterface
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.s3.configs.{S3 => S3Config}

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import java.time.Instant

final case class CurrentS3State(uploadId: String, parts: Seq[Part])

class BackupClient[T <: KafkaClientInterface](maybeS3Settings: Option[S3Settings])(implicit
    override val kafkaClientInterface: T,
    override val backupConfig: Backup,
    override val system: ActorSystem,
    s3Config: S3Config,
    s3Headers: S3Headers
) extends BackupClientInterface[T]
    with StrictLogging {

  override def empty: () => Future[Option[MultipartUploadResult]] = () => Future.successful(None)

  override type BackupResult = Option[MultipartUploadResult]

  override type CurrentState = CurrentS3State

  override def getCurrentUploadState(key: String): Future[Option[CurrentS3State]] = {
    implicit val ec: ExecutionContext = system.classicSystem.getDispatcher

    val baseListMultipart = S3.listMultipartUpload(s3Config.dataBucket, None)

    for {
      incompleteUploads <-
        maybeS3Settings
          .fold(baseListMultipart)(s3Settings => baseListMultipart.withAttributes(S3Attributes.settings(s3Settings)))
          .runWith(Sink.seq)
      keys = incompleteUploads.filter(_.key == key)
      result <- if (keys.isEmpty)
                  Future.successful(None)
                else {
                  val listMultipartUploads = keys match {
                    case Seq(single) =>
                      logger.info(
                        s"Found previous uploadId: ${single.uploadId} and bucket: ${s3Config.dataBucket} with key: ${single.key}"
                      )
                      single
                    case rest =>
                      val last = rest.minBy(_.initiated)(Ordering[Instant].reverse)
                      logger.warn(
                        s"Found multiple previously cancelled uploads for key: $key and bucket: ${s3Config.dataBucket}, picking uploadId: ${last.uploadId}"
                      )
                      last
                  }
                  val uploadId = listMultipartUploads.uploadId
                  val baseList = S3.listParts(s3Config.dataBucket, key, listMultipartUploads.uploadId)

                  for {
                    parts <- maybeS3Settings
                               .fold(baseList)(s3Settings => baseList.withAttributes(S3Attributes.settings(s3Settings)))
                               .runWith(Sink.seq)

                    finalParts = parts.lastOption match {
                                   case Some(part) if part.size >= akka.stream.alpakka.s3.scaladsl.S3.MinChunkSize =>
                                     parts
                                   case _ =>
                                     // We drop the last part here since its broken
                                     parts.dropRight(1)
                                 }
                  } yield Some(CurrentS3State(uploadId, finalParts.map(_.toPart)))
                }
    } yield result

  }

  private[s3] def failureSink
      : Sink[(FailedUploadPart, immutable.Iterable[kafkaClientInterface.CursorContext]), Future[Done]] = Sink
    .foreach[(FailedUploadPart, immutable.Iterable[kafkaClientInterface.CursorContext])] { case (failedPart, _) =>
      logger.warn(
        s"Failed to upload a chunk into S3 with bucket: ${failedPart.multipartUpload.bucket}, key: ${failedPart.multipartUpload.key}, uploadId: ${failedPart.multipartUpload.uploadId} and partNumber: ${failedPart.partNumber}",
        failedPart.exception
      )
    }

  private[s3] def successSink
      : Sink[(SuccessfulUploadPart, immutable.Iterable[kafkaClientInterface.CursorContext]), Future[Done]] =
    kafkaClientInterface.commitCursor
      .contramap[(SuccessfulUploadPart, immutable.Iterable[kafkaClientInterface.CursorContext])] { case (_, cursors) =>
        kafkaClientInterface.batchCursorContext(cursors)
      }

  private[s3] def kafkaBatchSink
      : Sink[(UploadPartResponse, immutable.Iterable[kafkaClientInterface.CursorContext]), NotUsed] = {

    val success = Flow[(UploadPartResponse, immutable.Iterable[kafkaClientInterface.CursorContext])]
      .collectType[(SuccessfulUploadPart, immutable.Iterable[kafkaClientInterface.CursorContext])]
      .wireTap { data =>
        val (part, _) = data
        logger.info(
          s"Committing kafka cursor for uploadId:${part.multipartUpload.uploadId} key: ${part.multipartUpload.key} and S3 part: ${part.partNumber}"
        )
      }
      .toMat(successSink)(Keep.none)

    val failure = Flow[(UploadPartResponse, immutable.Iterable[kafkaClientInterface.CursorContext])]
      .collectType[(FailedUploadPart, immutable.Iterable[kafkaClientInterface.CursorContext])]
      .toMat(failureSink)(Keep.none)

    Sink.combine(success, failure)(Broadcast(_))
  }

  override def backupToStorageSink(key: String,
                                   currentState: Option[CurrentS3State]
  ): Sink[(ByteString, kafkaClientInterface.CursorContext), Future[BackupResult]] = {

    // Note that chunkingParallelism is pointless for this usecase since we are directly streaming from Kafka.
    // Furthermore the real `KafkaClient` implementation uses `CommittableOffsetBatch` which is a global singleton so
    // we can't have concurrent updates to this data structure.

    val sink = currentState match {
      case Some(state) =>
        logger.info(
          s"Resuming previous upload with uploadId: ${state.uploadId} and bucket: ${s3Config.dataBucket} with key: $key"
        )
        S3.resumeMultipartUploadWithHeadersAndContext[kafkaClientInterface.CursorContext](
          s3Config.dataBucket,
          key,
          state.uploadId,
          state.parts,
          kafkaBatchSink,
          s3Headers = s3Headers,
          chunkingParallelism = 1
        )
      case None =>
        logger.info(
          s"Creating new upload with bucket: ${s3Config.dataBucket} and key: $key"
        )
        S3.multipartUploadWithHeadersAndContext[kafkaClientInterface.CursorContext](
          s3Config.dataBucket,
          key,
          kafkaBatchSink,
          s3Headers = s3Headers,
          chunkingParallelism = 1
        )
    }

    val base = sink.mapMaterializedValue(future => future.map(result => Some(result))(ExecutionContext.parasitic))
    maybeS3Settings.fold(base)(s3Settings => base.withAttributes(S3Attributes.settings(s3Settings)))
  }
}
