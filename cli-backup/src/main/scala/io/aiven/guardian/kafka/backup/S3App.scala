package io.aiven.guardian.kafka.backup

import io.aiven.guardian.kafka.backup.KafkaConsumer
import io.aiven.guardian.kafka.backup.s3.BackupClient
import io.aiven.guardian.kafka.s3.{Config => S3Config}
import org.apache.pekko

import pekko.stream.connectors.s3.S3Settings

trait S3App extends S3Config with BackupApp with App[KafkaConsumer] {
  lazy val s3Settings: S3Settings                             = S3Settings()
  implicit lazy val backupClient: BackupClient[KafkaConsumer] = new BackupClient[KafkaConsumer](Some(s3Settings))
}
