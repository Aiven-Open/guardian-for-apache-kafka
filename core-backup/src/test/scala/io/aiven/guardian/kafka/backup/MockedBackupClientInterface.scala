package io.aiven.guardian.kafka.backup

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import io.aiven.guardian.kafka.MockedKafkaClientInterface
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.models.ReducedConsumerRecord

/** A mocked `BackupClientInterface` which given a `kafkaClientInterface` allows you to
  * @param kafkaClientInterface
  * @param periodSlice
  */
class MockedBackupClientInterface(override val kafkaClientInterface: MockedKafkaClientInterface,
                                  periodSlice: FiniteDuration
) extends BackupClientInterface[MockedKafkaClientInterface] {

  /** The collection that receives the data as its being submitted where each value is the key along with the
    * `ByteString`. Use `mergeBackedUpData` to process `backedUpData` into a more convenient data structure once you
    * have finished writing to it
    */
  val backedUpData: Iterable[(String, ByteString)] = new ConcurrentLinkedQueue[(String, ByteString)]().asScala

  /** This method is intended to be called after you have written to it during a test.
    * @return
    *   `backupData` with all of the `ByteString` data merged for each unique key
    */
  def mergeBackedUpData: List[(String, ByteString)] = backedUpData
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
    * @param key
    *   The object key or filename for what is being backed up
    * @return
    *   A Sink that also provides a `BackupResult`
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
