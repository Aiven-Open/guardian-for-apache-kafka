package io.aiven.guardian.kafka.backup

import io.aiven.guardian.kafka.backup.configs.Backup
import pureconfig.generic.auto._
import pureconfig.ConfigSource

import scala.annotation.nowarn

trait Config {

  @nowarn("cat=lint-byname-implicit")
  implicit lazy val backupConfig: Backup = ConfigSource.default.at("backup").loadOrThrow[Backup]
}

object Config extends Config
