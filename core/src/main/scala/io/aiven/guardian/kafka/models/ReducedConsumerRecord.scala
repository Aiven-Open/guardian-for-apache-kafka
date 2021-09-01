package io.aiven.guardian.kafka.models

import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId

import org.apache.kafka.common.record.TimestampType

/** A `ConsumerRecord` that only contains the necessary data for guardian
  *
  * @param topic
  *   The kafka topic (same as `ConsumerRecord` `topic`)
  * @param offset
  *   The kafka offset (same as `ConsumerRecord` `offset`)
  * @param key
  *   Base64 encoded version of the original ConsumerRecord key as a byte array
  * @param value
  *   Base64 encoded version of the original ConsumerRecord value as a byte array
  * @param timestamp
  *   The timestamp value (same as `ConsumerRecord` `timestamp`)
  * @param timestampType
  *   The timestamp type (same as `ConsumerRecord` `timestampType`)
  */
final case class ReducedConsumerRecord(topic: String,
                                       offset: Long,
                                       key: String,
                                       value: String,
                                       timestamp: Long,
                                       timestampType: TimestampType
) {
  def toOffsetDateTime: OffsetDateTime =
    Instant.ofEpochMilli(this.timestamp).atZone(ZoneId.of("UTC")).toOffsetDateTime
}
