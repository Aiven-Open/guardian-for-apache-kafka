package io.aiven.guardian.kafka.restore.configs

import java.time.OffsetDateTime

/** @param fromWhen
  *   An optional datetime which only restores topics are are after or equal to that date
  * @param overrideTopics
  *   An optional map that allows you to translate topics that are backed up to a new topic name in the destination
  *   Kafka cluster. The key is the backed up topic name and the value is the new topic name. If this map doesn't
  *   contain a key for a topic then its backed up as normal.
  */
final case class Restore(fromWhen: Option[OffsetDateTime], overrideTopics: Option[Map[String, String]])

object Restore {
  def empty: Restore = Restore(None, None)
}
