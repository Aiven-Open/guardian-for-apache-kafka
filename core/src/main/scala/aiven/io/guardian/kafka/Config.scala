package aiven.io.guardian.kafka

import aiven.io.guardian.kafka.configs.KafkaCluster
import pureconfig.generic.auto._
import pureconfig.ConfigSource

trait Config {
  implicit lazy val kafkaClusterConfig: KafkaCluster =
    ConfigSource.default.at("kafka-cluster").loadOrThrow[KafkaCluster]
}

object Config extends Config
