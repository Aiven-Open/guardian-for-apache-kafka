package io.aiven.guardian.kafka.codecs

import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.circe._
import io.circe.syntax._
import org.apache.kafka.common.record.TimestampType

trait Circe {
  implicit val kafkaTimestampTypeDecoder: Decoder[TimestampType] = (c: HCursor) =>
    c.as[Int].flatMap { id =>
      TimestampType
        .values()
        .find(_.id == id)
        .toRight(DecodingFailure(s"No TimestampType with $id", c.history))
    }

  implicit val kafkaTimestampTypeEncoder: Encoder[TimestampType] = Encoder.instance[TimestampType](_.id.asJson)

  implicit val reducedConsumerRecordDecoder: Decoder[ReducedConsumerRecord] = Decoder.forProduct6(
    "topic",
    "offset",
    "key",
    "value",
    "timestamp",
    "timestamp_type"
  )(ReducedConsumerRecord.apply)

  implicit val reducedConsumerRecordEncoder: Encoder[ReducedConsumerRecord] = Encoder.forProduct6(
    "topic",
    "offset",
    "key",
    "value",
    "timestamp",
    "timestamp_type"
  )(x => ReducedConsumerRecord.unapply(x).get)
}

object Circe extends Circe
