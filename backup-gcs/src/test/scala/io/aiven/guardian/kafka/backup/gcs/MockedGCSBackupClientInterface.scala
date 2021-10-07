package io.aiven.guardian.kafka.backup.gcs

import akka.NotUsed
import akka.stream.alpakka.google.GoogleSettings
import akka.stream.alpakka.googlecloud.storage.GCSSettings
import akka.stream.scaladsl.Source
import io.aiven.guardian.kafka.MockedKafkaClientInterface
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.gcs.configs.{GCS => GCSConfig}
import io.aiven.guardian.kafka.models.ReducedConsumerRecord

import scala.concurrent.duration.FiniteDuration

class MockedGCSBackupClientInterface(
    kafkaData: List[ReducedConsumerRecord],
    periodSlice: FiniteDuration,
    gcsConfig: GCSConfig,
    maybeGoogleSettings: Option[GoogleSettings],
    maybeGCSSettings: Option[GCSSettings],
    sourceTransform: Option[
      Source[(ReducedConsumerRecord, Long), NotUsed] => Source[(ReducedConsumerRecord, Long), NotUsed]
    ] = None
) extends BackupClient(maybeGoogleSettings, maybeGCSSettings)(
      new MockedKafkaClientInterface(kafkaData, sourceTransform),
      Backup(periodSlice),
      gcsConfig
    )
