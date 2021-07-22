package aiven.io.guardian.kafka.backup

import aiven.io.guardian.kafka.backup.configs.Backup
import pureconfig.generic.auto._
import pureconfig.ConfigSource

trait Config {
  implicit lazy val backupConfig: Backup = ConfigSource.default.at("backup").loadOrThrow[Backup]
}

object Config extends Config
