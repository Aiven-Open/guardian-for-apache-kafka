package io.aiven.guardian.kafka.gcs

import scala.annotation.nowarn

import io.aiven.guardian.kafka.gcs.configs.GCS
import pureconfig._
import pureconfig.generic.auto._

trait Config {
  @nowarn("cat=lint-byname-implicit")
  implicit lazy val gcsConfig: GCS =
    ConfigSource.default.at("gcs-config").loadOrThrow[GCS]
}

object Config extends Config
