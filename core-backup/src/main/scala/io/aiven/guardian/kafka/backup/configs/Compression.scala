package io.aiven.guardian.kafka.backup.configs

import io.aiven.guardian.kafka.models.CompressionType

final case class Compression(`type`: CompressionType, level: Option[Int])
