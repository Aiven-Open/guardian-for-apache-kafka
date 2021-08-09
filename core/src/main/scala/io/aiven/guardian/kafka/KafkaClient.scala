package io.aiven.guardian.kafka

import io.aiven.guardian.kafka.configs.KafkaCluster
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.{Committable, CommittableOffset}
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Sink, SourceWithContext}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import java.util.Base64
import scala.concurrent.Future

/** A Kafka Client that uses Alpakka Kafka Consumer under the hood to create a stream of events from a Kafka cluster.
  * To configure the Alpakka Kafka Consumer use the standard typesafe configuration i.e. akka.kafka.consumer (note
  * that the `keySerializer` and `valueSerializer` are hardcoded so you cannot override this).
  * @param system A classic `ActorSystem`
  * @param kafkaClusterConfig Additional cluster configuration that is needed
  */
class KafkaClient()(implicit system: ActorSystem, kafkaClusterConfig: KafkaCluster)
    extends KafkaClientInterface
    with StrictLogging {
  override type CursorContext = Committable
  override type Control       = Consumer.Control

  if (kafkaClusterConfig.topics.isEmpty)
    logger.warn("Kafka Cluster configuration has no topics set")

  private[kafka] val consumerSettings =
    ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)

  private[kafka] val subscriptions = Subscriptions.topics(kafkaClusterConfig.topics)

  /** @return A `SourceWithContext` that returns a Kafka Stream which automatically handles committing of cursors
    */
  override val getSource: SourceWithContext[ReducedConsumerRecord, CommittableOffset, Consumer.Control] =
    Consumer
      .sourceWithOffsetContext(consumerSettings, subscriptions)
      .map(consumerRecord =>
        ReducedConsumerRecord(
          consumerRecord.topic(),
          consumerRecord.offset(),
          Base64.getEncoder.encodeToString(consumerRecord.key()),
          Base64.getEncoder.encodeToString(consumerRecord.value()),
          consumerRecord.timestamp(),
          consumerRecord.timestampType()
        )
      )

  private[kafka] val committerSettings: CommitterSettings = CommitterSettings(system)

  /** @return A `Sink` that allows you to commit a `CursorContext` to Kafka to signify you have processed a message
    */
  override val commitCursor: Sink[Committable, Future[Done]] = Committer.sink(committerSettings)
}
