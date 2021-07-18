package aiven.io.guardian.kafka.compaction.s3

import aiven.io.guardian.kafka.compaction.s3.models.StorageConfig
import pureconfig.generic.auto._
import pureconfig.ConfigSource

trait Config {
  implicit lazy val storageConfigS3: StorageConfig =
    ConfigSource.default.at("storage-config-s3").loadOrThrow[StorageConfig]
}

object Config extends Config
