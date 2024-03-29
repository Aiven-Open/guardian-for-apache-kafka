package io.aiven.guardian.kafka.restore

import io.aiven.guardian.cli.PekkoSettings
import io.aiven.guardian.kafka.restore.KafkaProducer
import io.aiven.guardian.kafka.restore.{Config => RestoreConfig}
import io.aiven.guardian.kafka.{Config => KafkaConfig}

trait RestoreApp extends RestoreConfig with KafkaConfig with PekkoSettings {
  implicit lazy val kafkaProducer: KafkaProducer = new KafkaProducer()
}
