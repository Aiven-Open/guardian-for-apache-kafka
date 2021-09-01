package io.aiven.guardian.kafka

import scala.annotation.nowarn

import io.aiven.guardian.kafka.configs.KafkaCluster
import pureconfig.ConfigSource
import pureconfig.generic.auto._

trait Config {

  @nowarn("cat=lint-byname-implicit")
  implicit lazy val kafkaClusterConfig: KafkaCluster =
    ConfigSource.default.at("kafka-cluster").loadOrThrow[KafkaCluster]
}

object Config extends Config
