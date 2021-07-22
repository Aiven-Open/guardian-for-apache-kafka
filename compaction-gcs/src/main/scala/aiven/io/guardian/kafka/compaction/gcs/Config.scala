package aiven.io.guardian.kafka.compaction.gcs

import aiven.io.guardian.kafka.compaction.gcs.models.StorageConfig
import pureconfig.generic.auto._
import pureconfig.ConfigSource

import scala.annotation.nowarn

trait Config {
  @nowarn("cat=lint-byname-implicit")
  implicit lazy val storageConfigGCS: StorageConfig =
    ConfigSource.default.at("storage-config-gcs").loadOrThrow[StorageConfig]
}

object Config extends Config
