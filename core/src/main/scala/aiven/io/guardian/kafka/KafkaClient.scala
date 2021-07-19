package aiven.io.guardian.kafka

import aiven.io.guardian.kafka.configs.KafkaCluster
import aiven.io.guardian.kafka.models.ReducedConsumerRecord
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.SourceWithContext
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import java.util.Base64

/** A Kafka Client that uses Alpakka Kafka Consumer under the hood to create a stream of events from a Kafka cluster.
  * To configure the Alpakka Kafka Consumer use the standard typesafe configuration i.e. akka.kafka.consumer (note
  * that the `keySerializer` and `valueSerializer` are hardcoded so you cannot override this).
  * @param system A classic `ActorSystem`
  * @param kafkaClusterConfig Additional cluster configuration that is needed
  */
class KafkaClient()(implicit system: ActorSystem, kafkaClusterConfig: KafkaCluster) extends KafkaClientInterface {

  private[kafka] val consumerSettings =
    ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)

  /** @return A `SourceWithContext` that returns a Kafka Stream which automatically handles committing of cursors
    */
  override val getSource
      : SourceWithContext[ReducedConsumerRecord, ConsumerMessage.CommittableOffset, Consumer.Control] =
    Consumer
      .sourceWithOffsetContext(consumerSettings, Subscriptions.topics(kafkaClusterConfig.topics))
      .map(consumerRecord =>
        ReducedConsumerRecord(
          consumerRecord.topic(),
          Base64.getEncoder.encodeToString(consumerRecord.key()),
          consumerRecord.value(),
          consumerRecord.timestamp(),
          consumerRecord.timestampType()
        )
      )
}
