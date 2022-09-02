package io.aiven.guardian.kafka.models

sealed trait CompressionType {
  val pretty: String
}

case object Gzip extends CompressionType {
  override val pretty: String = "Gzip"
}
