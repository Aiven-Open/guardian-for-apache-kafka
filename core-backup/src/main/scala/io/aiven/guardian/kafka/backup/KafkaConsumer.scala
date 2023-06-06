package io.aiven.guardian.kafka.backup

import com.typesafe.scalalogging.LazyLogging
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.backup.configs.ChronoUnitSlice
import io.aiven.guardian.kafka.backup.configs.PeriodFromFirst
import io.aiven.guardian.kafka.configs.KafkaCluster
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.pekko

import scala.collection.immutable
import scala.concurrent.Future
import scala.jdk.DurationConverters._

import java.util.Base64

import pekko.Done
import pekko.actor.ActorSystem
import pekko.kafka.CommitDelivery
import pekko.kafka.CommitterSettings
import pekko.kafka.ConsumerMessage.CommittableOffset
import pekko.kafka.ConsumerMessage.CommittableOffsetBatch
import pekko.kafka.ConsumerSettings
import pekko.kafka.Subscriptions
import pekko.kafka.scaladsl.Committer
import pekko.kafka.scaladsl.Consumer
import pekko.stream.scaladsl.Sink
import pekko.stream.scaladsl.SourceWithContext

/** A Kafka Client that uses Pekko Connectors Kafka Consumer under the hood to create a stream of events from a Kafka
  * cluster. To configure the Pekko Connectors Kafka Consumer use the standard typesafe configuration i.e.
  * pekko.kafka.consumer (note that the `keySerializer` and `valueSerializer` are hardcoded so you cannot override
  * this).
  * @param configureConsumer
  *   A way to configure the underlying Kafka consumer settings
  * @param configureCommitter
  *   A way to configure the underlying kafka committer settings
  * @param system
  *   A classic `ActorSystem`
  * @param kafkaClusterConfig
  *   Additional cluster configuration that is needed
  */
class KafkaConsumer(
    configureConsumer: Option[
      ConsumerSettings[Array[Byte], Array[Byte]] => ConsumerSettings[Array[Byte], Array[Byte]]
    ] = None,
    configureCommitter: Option[
      CommitterSettings => CommitterSettings
    ] = None
)(implicit system: ActorSystem, kafkaClusterConfig: KafkaCluster, backupConfig: Backup)
    extends KafkaConsumerInterface
    with LazyLogging {
  override type CursorContext        = CommittableOffset
  override type Control              = Consumer.Control
  override type MatCombineResult     = Consumer.DrainingControl[Done]
  override type BatchedCursorContext = CommittableOffsetBatch

  import KafkaConsumer._

  if (kafkaClusterConfig.topics.isEmpty)
    logger.warn("Kafka Cluster configuration has no topics set")

  private[kafka] val consumerSettings = {
    val base = ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)
    configureConsumer
      .fold(base)(block => block(base))
      .withProperties(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
      )
      .withCommitTimeout {
        val baseDuration = backupConfig.timeConfiguration match {
          case PeriodFromFirst(duration) => duration
          case ChronoUnitSlice(chronoUnit) =>
            chronoUnit.getDuration.toScala
        }

        baseDuration + backupConfig.commitTimeoutBufferWindow
      }
      .withGroupId(
        backupConfig.kafkaGroupId
      )
  }

  private[kafka] val subscriptions = Subscriptions.topics(kafkaClusterConfig.topics)

  /** @return
    *   A `SourceWithContext` that returns a Kafka Stream which automatically handles committing of cursors
    */
  override def getSource: SourceWithContext[ReducedConsumerRecord, CommittableOffset, Consumer.Control] =
    Consumer
      .sourceWithOffsetContext(consumerSettings, subscriptions)
      .map(consumerRecordToReducedConsumerRecord)

  private[kafka] val committerSettings: CommitterSettings = {
    val base = CommitterSettings(system)
    configureCommitter
      .fold(base)(block => block(base))
      .withDelivery(CommitDelivery.waitForAck)
  }

  /** @return
    *   A `Sink` that allows you to commit a `CursorContext` to Kafka to signify you have processed a message
    */
  override def commitCursor: Sink[CommittableOffsetBatch, Future[Done]] = Committer.sink(committerSettings)

  /** @return
    *   The result of this function gets directly passed into the `combine` parameter of
    *   [[pekko.stream.scaladsl.Source.toMat]]
    */
  override def matCombine: (Consumer.Control, Future[Done]) => Consumer.DrainingControl[Done] =
    Consumer.DrainingControl[Done].apply

  /** How to batch an immutable iterable of `CursorContext` into a `BatchedCursorContext`
    * @param cursors
    *   The cursors that need to be batched
    * @return
    *   A collection data structure that represents the batched cursors
    */
  override def batchCursorContext(cursors: immutable.Iterable[CommittableOffset]): CommittableOffsetBatch =
    CommittableOffsetBatch(cursors.toSeq)
}

object KafkaConsumer {
  def consumerRecordToReducedConsumerRecord(
      consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]
  ): ReducedConsumerRecord =
    ReducedConsumerRecord(
      consumerRecord.topic(),
      consumerRecord.partition(),
      consumerRecord.offset(),
      Option(consumerRecord.key()).map(byteArray => Base64.getEncoder.encodeToString(byteArray)),
      Base64.getEncoder.encodeToString(consumerRecord.value()),
      consumerRecord.timestamp(),
      consumerRecord.timestampType()
    )
}
