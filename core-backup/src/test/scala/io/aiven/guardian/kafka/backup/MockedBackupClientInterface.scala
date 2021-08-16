package io.aiven.guardian.kafka.backup

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink}
import akka.util.ByteString
import io.aiven.guardian.kafka.MockedKafkaClientInterface
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.models.ReducedConsumerRecord

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/** A mocked `BackupClientInterface` which given a `kafkaClientInterface` allows you to
  * @param kafkaClientInterface
  * @param periodSlice
  */
class MockedBackupClientInterface(override val kafkaClientInterface: MockedKafkaClientInterface,
                                  periodSlice: FiniteDuration
) extends BackupClientInterface[MockedKafkaClientInterface] {

  /** A Map where the key object key/filename and the value is the contents of the currently streamed
    * `KafkaReducedRecord` data
    */
  val backedUpData: Iterable[(String, ByteString)] = new ConcurrentLinkedQueue[(String, ByteString)]().asScala

  def mergeBackupData: List[(String, ByteString)] = backedUpData
    .groupBy { case (key, _) =>
      key
    }
    .view
    .mapValues { data =>
      data.toList.map { case (_, byteString) => byteString }.foldLeft(ByteString())(_ ++ _)
    }
    .toList

  override implicit lazy val backupConfig: Backup = Backup(
    periodSlice
  )

  /** Override this type to define the result of backing up data to a datasource
    */
  override type BackupResult = Done

  override def empty: () => Future[Done] = () => Future.successful(Done)

  /** Override this method to define how to backup a `ByteString` to a `DataSource`
    *
    * @param key The object key or filename for what is being backed up
    * @return A Sink that also provides a `BackupResult`
    */
  override def backupToStorageSink(key: String): Sink[ByteString, Future[Done]] = Sink.foreach { byteString =>
    backedUpData ++ Iterable((key, byteString))
  }

  def materializeBackupStreamPositions()(implicit
      system: ActorSystem
  ): Future[immutable.Iterable[(ReducedConsumerRecord, BackupStreamPosition)]] =
    calculateBackupStreamPositions(sourceWithPeriods(sourceWithFirstRecord)).asSource
      .map { case (data, _) =>
        data
      }
      .toMat(Sink.collection)(Keep.right)
      .run()
}

/** A `MockedBackupClientInterface` that also uses a mocked `KafkaClientInterface`
  */
class MockedBackupClientInterfaceWithMockedKafkaData(kafkaData: List[ReducedConsumerRecord],
                                                     periodSlice: FiniteDuration
) extends MockedBackupClientInterface(new MockedKafkaClientInterface(kafkaData), periodSlice)
