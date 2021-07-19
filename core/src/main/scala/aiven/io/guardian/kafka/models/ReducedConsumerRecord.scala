package aiven.io.guardian.kafka.models

import org.apache.kafka.common.record.TimestampType

/** A `ConsumerRecord` that only contains the necessary data for guardian
  *
  * @param topic The kafka topic (same as `ConsumerRecord` `topic`)
  * @param keyAsBase64 A Base64 encoded version of the original ConsumerRecord key as a byte array
  * @param data The data (or value as in `ConsumerRecord`). The format is the original byte array since we don't care
  *             about the message format
  * @param timestamp The timestamp value (same as `ConsumerRecord` `timestamp`)
  * @param timestampType The timestamp type (same as `ConsumerRecord` `timestampType`)
  */
final case class ReducedConsumerRecord(topic: String,
                                       keyAsBase64: String,
                                       data: Array[Byte],
                                       timestamp: Long,
                                       timestampType: TimestampType
)
