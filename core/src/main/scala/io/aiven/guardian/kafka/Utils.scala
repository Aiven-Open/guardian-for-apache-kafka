package io.aiven.guardian.kafka

import scala.annotation.tailrec

import java.time.OffsetDateTime
import java.time.format.DateTimeParseException

object Utils {

  private def parseToOffsetDateTime(string: String): Option[OffsetDateTime] =
    try
      Some(OffsetDateTime.parse(string))
    catch {
      case _: DateTimeParseException =>
        None
    }

  @tailrec
  def keyToOffsetDateTime(key: String): OffsetDateTime = {
    val withoutExtension = key.substring(0, key.lastIndexOf('.'))
    parseToOffsetDateTime(withoutExtension) match {
      case Some(offsetDateTime) => offsetDateTime
      case None                 => keyToOffsetDateTime(withoutExtension)
    }
  }
}
