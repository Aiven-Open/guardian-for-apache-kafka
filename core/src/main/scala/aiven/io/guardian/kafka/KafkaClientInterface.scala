package aiven.io.guardian.kafka

import aiven.io.guardian.kafka.models.ReducedConsumerRecord
import akka.kafka.ConsumerMessage
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.SourceWithContext

trait KafkaClientInterface {

  /** @return A `SourceWithContext` that returns a Kafka Stream which automatically handles committing of cursors
    */
  def getSource: SourceWithContext[ReducedConsumerRecord, ConsumerMessage.CommittableOffset, Consumer.Control]
}
