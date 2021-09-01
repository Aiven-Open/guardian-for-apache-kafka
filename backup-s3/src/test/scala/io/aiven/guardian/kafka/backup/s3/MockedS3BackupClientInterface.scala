package io.aiven.guardian.kafka.backup.s3

import scala.concurrent.duration.FiniteDuration

import akka.NotUsed
import akka.stream.alpakka.s3.S3Headers
import akka.stream.alpakka.s3.S3Settings
import akka.stream.scaladsl.Source
import io.aiven.guardian.kafka.MockedKafkaClientInterface
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.kafka.s3.configs.{S3 => S3Config}

class MockedS3BackupClientInterface(
    kafkaData: List[ReducedConsumerRecord],
    periodSlice: FiniteDuration,
    s3Config: S3Config,
    maybeS3Settings: Option[S3Settings],
    sourceTransform: Option[
      Source[(ReducedConsumerRecord, Long), NotUsed] => Source[(ReducedConsumerRecord, Long), NotUsed]
    ] = None
)(implicit val s3Headers: S3Headers)
    extends BackupClient(maybeS3Settings)(new MockedKafkaClientInterface(kafkaData, sourceTransform),
                                          Backup(periodSlice),
                                          s3Config,
                                          implicitly
    )
