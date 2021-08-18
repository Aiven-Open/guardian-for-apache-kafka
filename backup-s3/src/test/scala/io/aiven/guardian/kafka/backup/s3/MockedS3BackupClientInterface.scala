package io.aiven.guardian.kafka.backup.s3

import akka.stream.alpakka.s3.{S3Headers, S3Settings}
import io.aiven.guardian.kafka.MockedKafkaClientInterface
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.kafka.s3.configs.{S3 => S3Config}

import scala.concurrent.duration.FiniteDuration

class MockedS3BackupClientInterface(kafkaData: List[ReducedConsumerRecord],
                                    periodSlice: FiniteDuration,
                                    s3Config: S3Config,
                                    maybeS3Settings: Option[S3Settings]
)(implicit val s3Headers: S3Headers)
    extends BackupClient(maybeS3Settings)(new MockedKafkaClientInterface(kafkaData),
                                          Backup(periodSlice),
                                          s3Config,
                                          implicitly
    )
