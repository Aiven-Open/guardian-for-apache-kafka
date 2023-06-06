package io.aiven.guardian.kafka.backup.gcs

import io.aiven.guardian.kafka.backup.BackupClientInterface
import io.aiven.guardian.kafka.backup.KafkaConsumerInterface
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.gcs.configs.{GCS => GCSConfig}
import org.apache.pekko

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import pekko.actor.ActorSystem
import pekko.http.scaladsl.model.ContentTypes
import pekko.stream.connectors.google.GoogleAttributes
import pekko.stream.connectors.google.GoogleSettings
import pekko.stream.connectors.googlecloud.storage.StorageObject
import pekko.stream.connectors.googlecloud.storage.scaladsl.GCStorage
import pekko.stream.scaladsl.Sink
import pekko.util.ByteString

// TODO: GCS implementation currently does not work correctly because of inability of current GCS implementation in
// Pekko Connectors to allow us to commit Kafka cursor whenever chunks are uploaded
class BackupClient[T <: KafkaConsumerInterface](maybeGoogleSettings: Option[GoogleSettings])(implicit
    override val kafkaClientInterface: T,
    override val backupConfig: Backup,
    override val system: ActorSystem,
    gcsConfig: GCSConfig
) extends BackupClientInterface[T] {

  override def empty: () => Future[Option[StorageObject]] = () => Future.successful(None)

  override type BackupResult = Option[StorageObject]

  override type State = Nothing

  override def getCurrentUploadState(key: String): Future[UploadStateResult] =
    Future.successful(UploadStateResult.empty)

  override def backupToStorageTerminateSink(
      previousState: PreviousState
  ): Sink[ByteString, Future[Option[StorageObject]]] = {
    val base = GCStorage
      .resumableUpload(gcsConfig.dataBucket, previousState.previousKey, ContentTypes.`application/json`)
      .mapMaterializedValue(future => future.map(result => Some(result))(ExecutionContext.parasitic))

    maybeGoogleSettings
      .fold(base)(googleSettings => base.withAttributes(GoogleAttributes.settings(googleSettings)))
  }

  override def backupToStorageSink(key: String,
                                   currentState: Option[Nothing]
  ): Sink[(ByteString, kafkaClientInterface.CursorContext), Future[BackupResult]] = {
    val base = GCStorage
      .resumableUpload(gcsConfig.dataBucket, key, ContentTypes.`application/json`)
      .mapMaterializedValue(future => future.map(result => Some(result))(ExecutionContext.parasitic))

    maybeGoogleSettings
      .fold(base)(googleSettings => base.withAttributes(GoogleAttributes.settings(googleSettings)))
      .contramap[(ByteString, kafkaClientInterface.CursorContext)] { case (byteString, _) =>
        byteString
      }
  }
}
