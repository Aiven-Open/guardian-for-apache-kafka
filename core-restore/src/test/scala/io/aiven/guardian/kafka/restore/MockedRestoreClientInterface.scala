package io.aiven.guardian.kafka.restore

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import io.aiven.guardian.kafka.configs.KafkaCluster
import io.aiven.guardian.kafka.restore.configs.Restore

import scala.concurrent.Future

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
