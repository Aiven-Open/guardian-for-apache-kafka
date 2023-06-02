package io.aiven.guardian.kafka.backup

import io.aiven.guardian.cli.PekkoSettings
import io.aiven.guardian.kafka.backup.KafkaConsumer
import io.aiven.guardian.kafka.backup.{Config => BackupConfig}
import io.aiven.guardian.kafka.{Config => KafkaConfig}

trait BackupApp extends BackupConfig with KafkaConfig with PekkoSettings {
  implicit lazy val kafkaClient: KafkaConsumer = new KafkaConsumer()
}
