package io.aiven.guardian.kafka.backup.s3

import io.aiven.guardian.kafka.backup.KafkaConsumer
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.configs.KafkaCluster
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import org.apache.pekko

import pekko.actor.ActorSystem
import pekko.kafka.CommitterSettings
import pekko.kafka.ConsumerMessage
import pekko.kafka.ConsumerSettings
import pekko.kafka.scaladsl.Consumer
import pekko.stream.SharedKillSwitch
import pekko.stream.scaladsl.SourceWithContext

class KafkaConsumerWithKillSwitch(
    configureConsumer: Option[
      ConsumerSettings[Array[Byte], Array[Byte]] => ConsumerSettings[Array[Byte], Array[Byte]]
    ] = None,
    configureCommitter: Option[
      CommitterSettings => CommitterSettings
    ] = None,
    killSwitch: SharedKillSwitch
)(implicit system: ActorSystem, kafkaClusterConfig: KafkaCluster, backupConfig: Backup)
    extends KafkaConsumer(configureConsumer, configureCommitter) {
  override def getSource
      : SourceWithContext[ReducedConsumerRecord, ConsumerMessage.CommittableOffset, Consumer.Control] =
    super.getSource.via(killSwitch.flow)
}
