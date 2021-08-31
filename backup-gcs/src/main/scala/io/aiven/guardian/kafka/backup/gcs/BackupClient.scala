package io.aiven.guardian.kafka.backup.gcs

import akka.http.scaladsl.model.ContentTypes
import akka.stream.alpakka.google.{GoogleAttributes, GoogleSettings}
import akka.stream.alpakka.googlecloud.storage.StorageObject
import akka.stream.alpakka.googlecloud.storage.scaladsl.GCStorage
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import io.aiven.guardian.kafka.KafkaClientInterface
import io.aiven.guardian.kafka.backup.BackupClientInterface
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.gcs.configs.{GCS => GCSConfig}

import scala.concurrent.{ExecutionContext, Future}

class BackupClient[T <: KafkaClientInterface](maybeGoogleSettings: Option[GoogleSettings])(implicit
    override val kafkaClientInterface: T,
    override val backupConfig: Backup,
    gcsConfig: GCSConfig
) extends BackupClientInterface[T] {

  override def empty: () => Future[Option[StorageObject]] = () => Future.successful(None)

  override type BackupResult = Option[StorageObject]

  override def backupToStorageSink(key: String): Sink[ByteString, Future[BackupResult]] = {
    val base = GCStorage
      .resumableUpload(gcsConfig.dataBucket, key, ContentTypes.`application/json`)
      .mapMaterializedValue(future => future.map(result => Some(result))(ExecutionContext.parasitic))

    maybeGoogleSettings.fold(base)(googleSettings => base.withAttributes(GoogleAttributes.settings(googleSettings)))
  }

}
