package io.aiven.guardian.kafka.backup

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.aiven.guardian.kafka.Utils._
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.backup.configs.TimeConfiguration
import io.aiven.guardian.kafka.models.ReducedConsumerRecord

import scala.collection.immutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import java.time.OffsetDateTime
import java.util.concurrent.ConcurrentLinkedQueue

/** A mocked `BackupClientInterface` which given a `kafkaClientInterface` allows you to
  * @param kafkaClientInterface
  * @param periodSlice
  */
class MockedBackupClientInterface(override val kafkaClientInterface: MockedKafkaClientInterface,
                                  timeConfiguration: TimeConfiguration
)(implicit override val system: ActorSystem)
    extends BackupClientInterface[MockedKafkaClientInterface] {

  import MockedBackupClientInterface._

  /** The collection that receives the data as its being submitted where each value is the key along with the
    * `ByteString`. Use `mergeBackedUpData` to process `backedUpData` into a more convenient data structure once you
    * have finished writing to it
    */
  val backedUpData: ConcurrentLinkedQueue[(String, ByteString)] = new ConcurrentLinkedQueue[(String, ByteString)]()

  /** This method is intended to be called after you have written to it during a test.
    * @param terminate
    *   Whether to terminate the ByteString with `null]` so its valid parsable JSON
    * @param sort
    *   Whether to sort the outputting collection. There are sometimes corner cases when dealing with small sets of
    *   static data that the outputted stream can be unordered.
    * @return
    *   `backupData` with all of the `ByteString` data merged for each unique key
    */
  def mergeBackedUpData(terminate: Boolean = true, sort: Boolean = true): List[(String, ByteString)] = {
    val base = backedUpData.asScala
      .orderedGroupBy { case (key, _) =>
        key
      }
      .view
      .mapValues { data =>
        val complete = data.map { case (_, byteString) => byteString }.foldLeft(ByteString())(_ ++ _)
        if (terminate)
          if (complete.utf8String.endsWith("},"))
            complete ++ ByteString("null]")
          else
            complete
        else
          complete
      }
      .toList
    if (sort)
      base.sortBy { case (key, _) =>
        val withoutExtension = key.substring(0, key.lastIndexOf('.'))
        OffsetDateTime.parse(withoutExtension).getNano
      }
    else
      base
  }

  def clear(): Unit = backedUpData.clear()

  override implicit lazy val backupConfig: Backup = Backup(
    KafkaGroupId,
    timeConfiguration
  )

  /** Override this type to define the result of backing up data to a datasource
    */
  override type BackupResult = Done

  override type CurrentState = Nothing

  override def getCurrentUploadState(key: String): Future[Option[Nothing]] = Future.successful(None)

  override def empty: () => Future[Done] = () => Future.successful(Done)

  /** Override this method to define how to backup a `ByteString` to a `DataSource`
    *
    * @param key
    *   The object key or filename for what is being backed up
    * @return
    *   A Sink that also provides a `BackupResult`
    */
  override def backupToStorageSink(key: String,
                                   currentState: Option[Nothing]
  ): Sink[(ByteString, kafkaClientInterface.CursorContext), Future[Done]] =
    Sink.foreach { case (byteString, _) =>
      backedUpData.add((key, byteString))
    }

  def materializeBackupStreamPositions()(implicit
      system: ActorSystem
  ): Future[immutable.Iterable[RecordElement]] =
    calculateBackupStreamPositions(sourceWithPeriods(sourceWithFirstRecord))
      .toMat(Sink.collection)(Keep.right)
      .run()
}

object MockedBackupClientInterface {

  /** A Kafka group id that is used in testing. When used in pure mocks this value is simply ignored (since there is no
    * Kafka cluster) where as when used against actual test Kafka clusters this is the consumer group that is used
    */
  val KafkaGroupId: String = "test"
}

/** A `MockedBackupClientInterface` that also uses a mocked `KafkaClientInterface`
  */
class MockedBackupClientInterfaceWithMockedKafkaData(kafkaData: Source[ReducedConsumerRecord, NotUsed],
                                                     timeConfiguration: TimeConfiguration
)(implicit override val system: ActorSystem)
    extends MockedBackupClientInterface(new MockedKafkaClientInterface(kafkaData), timeConfiguration)
