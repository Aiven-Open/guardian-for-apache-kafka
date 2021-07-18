package aiven.io.guardian.kafka.models

import java.time.OffsetDateTime

final case class KafkaRow(topic: String,
                          key: String,
                          blob: Array[Byte],
                          insert: OffsetDateTime,
                          received: OffsetDateTime
)
