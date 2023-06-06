package io.aiven.guardian.kafka.restore

import io.aiven.guardian.kafka.configs.KafkaCluster
import io.aiven.guardian.kafka.restore.configs.Restore
import org.apache.pekko

import scala.concurrent.Future

import pekko.NotUsed
import pekko.actor.ActorSystem
import pekko.stream.scaladsl.Flow
import pekko.util.ByteString

class MockedRestoreClientInterface(backupData: Map[String, ByteString])(implicit
    override val kafkaProducerInterface: MockedKafkaProducerInterface,
    override val restoreConfig: Restore,
    override val kafkaClusterConfig: KafkaCluster,
    override val system: ActorSystem
) extends RestoreClientInterface[MockedKafkaProducerInterface] {

  override def retrieveBackupKeys: Future[List[String]] = Future.successful(
    backupData.keys.toList
  )

  override def downloadFlow: Flow[String, ByteString, NotUsed] = Flow.fromFunction { key: String =>
    backupData(key)
  }
}
