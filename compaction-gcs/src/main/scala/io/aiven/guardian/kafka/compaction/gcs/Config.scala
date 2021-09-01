package io.aiven.guardian.kafka.compaction.gcs

import scala.annotation.nowarn

import io.aiven.guardian.kafka.compaction.gcs.models.StorageConfig
import pureconfig.ConfigSource
import pureconfig.generic.auto._

trait Config {
  @nowarn("cat=lint-byname-implicit")
  implicit lazy val storageConfigGCS: StorageConfig =
    ConfigSource.default.at("storage-config-gcs").loadOrThrow[StorageConfig]
}

object Config extends Config
