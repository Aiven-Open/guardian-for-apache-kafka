package io.aiven.guardian.kafka.backup.s3

import io.aiven.guardian.kafka.backup.MockedBackupClientInterface
import io.aiven.guardian.kafka.backup.MockedKafkaConsumerInterface
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.backup.configs.TimeConfiguration
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.kafka.s3.configs.{S3 => S3Config}
import org.apache.pekko

import scala.concurrent.duration._
import scala.language.postfixOps

import pekko.NotUsed
import pekko.actor.ActorSystem
import pekko.stream.connectors.s3.S3Headers
import pekko.stream.connectors.s3.S3Settings
import pekko.stream.scaladsl.Source

class MockedS3BackupClientInterface(
    kafkaData: Source[ReducedConsumerRecord, NotUsed],
    timeConfiguration: TimeConfiguration,
    s3Config: S3Config,
    maybeS3Settings: Option[S3Settings]
)(implicit val s3Headers: S3Headers, system: ActorSystem)
    extends BackupClient(
      maybeS3Settings
    )(
      new MockedKafkaConsumerInterface(kafkaData),
      Backup(MockedBackupClientInterface.KafkaGroupId, timeConfiguration, 10 seconds, None),
      implicitly,
      s3Config,
      implicitly
    )
