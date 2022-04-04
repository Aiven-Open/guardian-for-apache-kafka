package io.aiven.guardian.kafka

import java.time.OffsetDateTime

object Utils {
  def keyToOffsetDateTime(key: String): OffsetDateTime = {
    val withoutExtension = key.substring(0, key.lastIndexOf('.'))
    OffsetDateTime.parse(withoutExtension)
  }
}
