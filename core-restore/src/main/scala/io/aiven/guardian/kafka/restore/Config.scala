package io.aiven.guardian.kafka.restore

import io.aiven.guardian.kafka.restore.configs.Restore
import pureconfig._
import pureconfig.configurable._
import pureconfig.generic.auto._

import scala.annotation.nowarn

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

trait Config {
  implicit val localDateConvert: ConfigConvert[OffsetDateTime] = offsetDateTimeConfigConvert(
    DateTimeFormatter.ISO_OFFSET_DATE_TIME
  )

  @nowarn("cat=lint-byname-implicit")
  implicit lazy val restoreConfig: Restore = ConfigSource.default.at("restore").loadOrThrow[Restore]
}

object Config extends Config
