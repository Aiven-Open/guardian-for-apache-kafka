package io.aiven.guardian.kafka.restore

import akka.stream.SharedKillSwitch
import io.aiven.guardian.cli.AkkaSettings
import io.aiven.guardian.kafka.restore.KafkaProducer
import io.aiven.guardian.kafka.restore.{Config => RestoreConfig}
import io.aiven.guardian.kafka.{Config => KafkaConfig}

trait RestoreApp extends RestoreConfig with KafkaConfig with AkkaSettings {
  val maybeKillSwitch: Option[SharedKillSwitch]
  implicit lazy val kafkaProducer: KafkaProducer = new KafkaProducer
}
