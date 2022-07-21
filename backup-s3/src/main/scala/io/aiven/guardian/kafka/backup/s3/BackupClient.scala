package io.aiven.guardian.kafka.backup.s3

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.SinkShape
import akka.stream.alpakka.s3._
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl._
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import io.aiven.guardian.kafka.backup.BackupClientInterface
import io.aiven.guardian.kafka.backup.KafkaClientInterface
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.s3.configs.{S3 => S3Config}

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import java.time.Instant

class BackupClient[T <: KafkaClientInterface](maybeS3Settings: Option[S3Settings])(implicit
    override val kafkaClientInterface: T,
    override val backupConfig: Backup,
    override val system: ActorSystem,
    s3Config: S3Config,
    s3Headers: S3Headers
) extends BackupClientInterface[T]
    with LazyLogging {
  import BackupClient._

  override def empty: () => Future[Option[MultipartUploadResult]] = () => Future.successful(None)

  override type BackupResult = Option[MultipartUploadResult]

  override type State = CurrentS3State

  private[backup] def checkObjectExists(key: String): Future[Boolean] = {
    val base = S3.getObjectMetadata(s3Config.dataBucket, key)

    maybeS3Settings
      .fold(base)(s3Settings => base.withAttributes(S3Attributes.settings(s3Settings)))
      .runWith(Sink.headOption)
      .map(
        _.exists(_.isDefined)
      )(ExecutionContext.parasitic)
  }

  /** @return
    *   If an in progress multipart upload exists for the key than returns that result in a [[Some]] otherwise it
    *   returns an empty [[None]].
    */
  private[backup] def getPartsFromUpload(key: String, uploadId: String)(implicit
      executionContext: ExecutionContext
  ): Future[Option[Seq[ListPartsResultParts]]] = {
    val baseList = S3.listParts(s3Config.dataBucket, key, uploadId)

    maybeS3Settings
      .fold(baseList)(s3Settings => baseList.withAttributes(S3Attributes.settings(s3Settings)))
      .runWith(Sink.seq)
      .map(x => Some(x))(ExecutionContext.parasitic)
      .recover {
        // Its possible for S3 to return an in progress multipart upload for an upload that has already been finished.
        // This exception is thrown in such a case so lets recover and act as if there was no multipart uploads in progress
        // for this key.
        case e: S3Exception
            if e.getMessage.contains(
              "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed."
            ) =>
          logger.debug(s"Last upload with uploadId: $uploadId and key: $key doesn't actually exist", e)
          None
      }
  }

  private def extractStateFromUpload(uploads: Seq[ListMultipartUploadResultUploads], current: Boolean)(implicit
      executionContext: ExecutionContext
  ): Future[Option[(CurrentS3State, String)]] =
    for {
      // Its possible for S3 to return an in progress multipart upload even though the upload has already been finished
      // so lets filter out any multipart uploads that already happen to exist as objects in S3
      uploadsWithExists <- Future.sequence(uploads.map { upload =>
                             checkObjectExists(upload.key).map(exists => (upload, exists))
                           })
      filteredUploads = uploadsWithExists
                          .filter {
                            case (_, false) => true
                            case (upload, true) =>
                              logger.debug(
                                s"Found already existing stale upload with uploadId: ${upload.uploadId} and key: ${upload.key}, ignoring"
                              )
                              false
                          }
                          .map { case (upload, _) => upload }
      result <- if (filteredUploads.isEmpty)
                  Future.successful(None)
                else {
                  val listMultipartUploads = filteredUploads match {
                    case Seq(single) =>
                      if (current)
                        logger.info(
                          s"Found current uploadId: ${single.uploadId} and bucket: ${s3Config.dataBucket} with key: ${single.key}"
                        )
                      else
                        logger.info(
                          s"Found previous uploadId: ${single.uploadId} and bucket: ${s3Config.dataBucket} with key: ${single.key}"
                        )
                      single
                    case rest =>
                      val last = rest.maxBy(_.initiated)(Ordering[Instant])
                      if (current)
                        logger.warn(
                          s"Found multiple currently cancelled uploads for key: ${last.key} and bucket: ${s3Config.dataBucket}, picking uploadId: ${last.uploadId}"
                        )
                      else
                        logger.warn(
                          s"Found multiple previously cancelled uploads for key: ${last.key} and bucket: ${s3Config.dataBucket}, picking uploadId: ${last.uploadId}"
                        )
                      last
                  }
                  val uploadId = listMultipartUploads.uploadId
                  val key      = listMultipartUploads.key

                  for {
                    maybeParts <- getPartsFromUpload(key, uploadId)
                  } yield maybeParts.map { parts =>
                    val finalParts = parts.lastOption match {
                      case Some(part) if part.size >= akka.stream.alpakka.s3.scaladsl.S3.MinChunkSize =>
                        parts
                      case _ =>
                        // We drop the last part here since its broken
                        parts.dropRight(1)
                    }
                    (CurrentS3State(uploadId, finalParts.map(_.toPart)), key)
                  }
                }
    } yield result

  def getCurrentUploadState(key: String): Future[UploadStateResult] = {
    implicit val ec: ExecutionContext = system.dispatcher

    val baseListMultipart = S3.listMultipartUpload(s3Config.dataBucket, None)

    for {
      incompleteUploads <-
        maybeS3Settings
          .fold(baseListMultipart)(s3Settings => baseListMultipart.withAttributes(S3Attributes.settings(s3Settings)))
          .runWith(Sink.seq)
      incompleteUploadsWithExistingParts <-
        Future
          .sequence(
            incompleteUploads.map(upload =>
              getPartsFromUpload(upload.key, upload.uploadId)
                .map(result => (upload, result))(ExecutionContext.parasitic)
            )
          )
          .map(_.collect {
            case (upload, Some(parts)) if parts.nonEmpty => upload
          })(ExecutionContext.parasitic)
      (currentKeys, previousKeys) = incompleteUploadsWithExistingParts.partition(_.key == key)
      current <- if (currentKeys.nonEmpty)
                   extractStateFromUpload(currentKeys, current = true)
                 else
                   Future.successful(None)
      previous <- if (previousKeys.nonEmpty)
                    extractStateFromUpload(previousKeys, current = false)
                  else
                    Future.successful(None)

    } yield UploadStateResult(current.map(_._1),
                              previous.map { case (state, previousKey) => PreviousState(state, previousKey) }
    )

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
      .contramap[(SuccessfulUploadPart, immutable.Iterable[kafkaClientInterface.CursorContext])] {
        case (part, cursors) =>
          logger.info(
            s"Committing kafka cursor for uploadId: ${part.multipartUpload.uploadId} key: ${part.multipartUpload.key} and S3 part: ${part.partNumber}"
          )
          kafkaClientInterface.batchCursorContext(cursors)
      }

  private[s3] def kafkaBatchSink
      : Sink[(UploadPartResponse, immutable.Iterable[kafkaClientInterface.CursorContext]), NotUsed] =
    // See https://doc.akka.io/docs/akka/current/stream/operators/Partition.html for an explanation on Partition
    Sink.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val partition = builder.add(
        new Partition[(UploadPartResponse, immutable.Iterable[kafkaClientInterface.CursorContext])](
          outputPorts = 2,
          {
            case (_: SuccessfulUploadPart, _) => 0
            case (_: FailedUploadPart, _)     => 1
          },
          eagerCancel = true
        )
      )
      partition.out(0) ~> successSink
        .contramap[(UploadPartResponse, immutable.Iterable[kafkaClientInterface.CursorContext])] {
          case (response, value) => (response.asInstanceOf[SuccessfulUploadPart], value)
        }
      partition.out(1) ~> failureSink
        .contramap[(UploadPartResponse, immutable.Iterable[kafkaClientInterface.CursorContext])] {
          case (response, value) => (response.asInstanceOf[FailedUploadPart], value)
        }
      SinkShape(partition.in)
    })

  override def backupToStorageTerminateSink(
      previousState: PreviousState
  ): Sink[ByteString, Future[BackupResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher

    // Due to eventual consistency its possible to upload a part for a in progress multipart upload for an upload that
    // has in fact already finished. If this is the case then lets just recover from the expected error in S3 in
    // addition to checking if the upload has already been turned into an S3 object.
    RestartSink.withBackoff(
      s3Config.errorRestartSettings
    ) { () =>
      Sink.lazyFutureSink { () =>
        for {
          exists <- checkObjectExists(previousState.previousKey)
        } yield
        // The backupToStorageTerminateSink gets called in response to finding in progress multipart uploads. If an S3 object exists
        // the same key that means that in fact the upload has already been completed so in this case lets not do anything
        if (exists) {
          logger.debug(
            s"Previous upload with uploadId: ${previousState.state.uploadId} and key: ${previousState.previousKey} doesn't actually exist, skipping terminating"
          )
          Sink.ignore
        } else {
          logger.info(
            s"Terminating and completing previous backup with key: ${previousState.previousKey} and uploadId: ${previousState.state.uploadId}"
          )
          val sink = S3
            .resumeMultipartUploadWithHeaders(
              s3Config.dataBucket,
              previousState.previousKey,
              previousState.state.uploadId,
              previousState.state.parts,
              s3Headers = s3Headers,
              chunkingParallelism = 1
            )

          val base =
            sink.mapMaterializedValue(future => future.map(result => Some(result))(ExecutionContext.parasitic))

          maybeS3Settings
            .fold(base)(s3Settings => base.withAttributes(S3Attributes.settings(s3Settings)))
        }

      }
    }
  }.mapMaterializedValue(_ => Future.successful(None))

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

object BackupClient {
  final case class CurrentS3State(uploadId: String, parts: Seq[Part])
}
