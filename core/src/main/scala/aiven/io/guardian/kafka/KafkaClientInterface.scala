package aiven.io.guardian.kafka

import aiven.io.guardian.kafka.models.ReducedConsumerRecord
import akka.kafka.ConsumerMessage
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.SourceWithContext

trait KafkaClientInterface {

  /** The type of the context to pass around. In context of a Kafka consumer, this typically holds offset data to be
    * automatically committed
    */
  type Context

  /** A materializer that defines how to commit the cursor stored in `Context`
    */
  type Mat

  /** @return A `SourceWithContext` that returns a Kafka Stream which automatically handles committing of cursors
    */
  def getSource: SourceWithContext[ReducedConsumerRecord, Context, Mat]
}
