package io.aiven.guardian.kafka.backup

import io.aiven.guardian.cli.AkkaSettings
import io.aiven.guardian.kafka.backup.KafkaConsumer
import io.aiven.guardian.kafka.backup.{Config => BackupConfig}
import io.aiven.guardian.kafka.{Config => KafkaConfig}

trait BackupApp extends BackupConfig with KafkaConfig with AkkaSettings {
  implicit lazy val kafkaClient: KafkaConsumer = new KafkaConsumer()
}
