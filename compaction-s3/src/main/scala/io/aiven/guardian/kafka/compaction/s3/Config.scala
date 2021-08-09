package io.aiven.guardian.kafka.compaction.s3

import io.aiven.guardian.kafka.compaction.s3.models.StorageConfig
import pureconfig.generic.auto._
import pureconfig.ConfigSource

import scala.annotation.nowarn

trait Config {
  @nowarn("cat=lint-byname-implicit")
  implicit lazy val storageConfigS3: StorageConfig =
    ConfigSource.default.at("storage-config-s3").loadOrThrow[StorageConfig]
}

object Config extends Config
