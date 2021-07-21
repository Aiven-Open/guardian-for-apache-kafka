package aiven.io.guardian.kafka

import aiven.io.guardian.kafka.models.ReducedConsumerRecord
import akka.stream.scaladsl.SourceWithContext

trait KafkaClientInterface {

  /** The type of the context to pass around. In context of a Kafka consumer, this typically holds offset data to be
    * automatically committed
    */
  type Context

  /** The type that represents how to control the given stream, i.e. if you want to shut it down or add metrics
    */
  type Control

  /** @return A `SourceWithContext` that returns a Kafka Stream which automatically handles committing of cursors
    */
  def getSource: SourceWithContext[ReducedConsumerRecord, Context, Control]
}
