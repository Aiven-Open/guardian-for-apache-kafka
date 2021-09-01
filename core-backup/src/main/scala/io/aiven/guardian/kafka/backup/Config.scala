package io.aiven.guardian.kafka.backup

import scala.annotation.nowarn

import io.aiven.guardian.kafka.backup.configs.Backup
import pureconfig.ConfigSource
import pureconfig.generic.auto._

trait Config {

  @nowarn("cat=lint-byname-implicit")
  implicit lazy val backupConfig: Backup = ConfigSource.default.at("backup").loadOrThrow[Backup]
}

object Config extends Config
