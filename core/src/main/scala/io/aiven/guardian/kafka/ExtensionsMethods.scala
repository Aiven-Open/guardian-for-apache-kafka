package io.aiven.guardian.kafka

import java.time.OffsetDateTime

object ExtensionsMethods {

  implicit final class OffsetDateTimeMethods(value: OffsetDateTime) {
    def >(other: OffsetDateTime): Boolean  = value.compareTo(other) > 0
    def >=(other: OffsetDateTime): Boolean = value.compareTo(other) > 0 || value == other
    def <(other: OffsetDateTime): Boolean  = value.compareTo(other) < 0
    def <=(other: OffsetDateTime): Boolean = value.compareTo(other) < 0 || value == other
  }

}
