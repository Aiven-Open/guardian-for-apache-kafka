package io.aiven.guardian.kafka.backup

import io.aiven.guardian.kafka.TestUtils._
import io.aiven.guardian.kafka.Utils
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.backup.configs.TimeConfiguration
import io.aiven.guardian.kafka.backup.configs.{Compression => CompressionModel}
import io.aiven.guardian.kafka.models.BackupObjectMetadata
import io.aiven.guardian.kafka.models.CompressionType
import io.aiven.guardian.kafka.models.Gzip
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import org.apache.pekko

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentLinkedQueue

import pekko.Done
import pekko.NotUsed
import pekko.actor.ActorSystem
import pekko.stream.scaladsl.Compression
import pekko.stream.scaladsl.Flow
import pekko.stream.scaladsl.Keep
import pekko.stream.scaladsl.Sink
import pekko.stream.scaladsl.Source
import pekko.util.ByteString

/** A mocked `BackupClientInterface` which given a `kafkaClientInterface` allows you to
  *
  * @param kafkaClientInterface
  * @param timeConfiguration
  * @param backedUpData
  *   The collection that receives the data as its being submitted where each value is the key along with the
  *   `ByteString`. Use `mergeBackedUpData` to process `backedUpData` into a more convenient data structure once you
  *   have finished writing to it
  */
class MockedBackupClientInterface(override val kafkaClientInterface: MockedKafkaConsumerInterface,
                                  timeConfiguration: TimeConfiguration,
                                  compression: Option[CompressionModel] = None,
                                  backedUpData: ConcurrentLinkedQueue[(String, ByteString)] =
                                    new ConcurrentLinkedQueue[(String, ByteString)]()
)(implicit override val system: ActorSystem)
    extends BackupClientInterface[MockedKafkaConsumerInterface] {

  import MockedBackupClientInterface._

  /** This method is intended to be called after you have written to it during a test.
    * @param terminate
    *   Whether to terminate the ByteString with `null]` so its valid parsable JSON
    * @param sort
    *   Whether to sort the outputting collection. There are sometimes corner cases when dealing with small sets of
    *   static data that the outputted stream can be unordered.
    * @return
    *   `backupData` with all of the `ByteString` data merged for each unique key
    */
  def mergeBackedUpData(terminate: Boolean = true,
                        sort: Boolean = true,
                        compression: Option[CompressionType] = None
  ): List[(String, ByteString)] = {
    val base = backedUpData.asScala
      .orderedGroupBy { case (key, _) =>
        key
      }
      .view
      .map { case (key, data) =>
        val joined = joinByteString(data.map { case (_, byteString) => byteString })
        val complete =
          // Only bother decompressing when needed since some tests can have mixed backups
          if (BackupObjectMetadata.fromKey(key).compression.isDefined)
            decompress(joined, compression)
          else
            joined

        val finalData =
          if (terminate)
            if (complete.utf8String.endsWith("},"))
              complete ++ ByteString("null]")
            else
              complete
          else
            complete
        (key, finalData)
      }
      .toList
    if (sort)
      base.sortBy { case (key, _) =>
        Utils.keyToOffsetDateTime(key)
      }
    else
      base
  }

  def clear(): Unit = backedUpData.clear()

  override implicit lazy val backupConfig: Backup = Backup(
    KafkaGroupId,
    timeConfiguration,
    10 seconds,
    compression
  )

  /** Override this type to define the result of backing up data to a datasource
    */
  override type BackupResult = Done

  override type State = Unit

  override def getCurrentUploadState(key: String): Future[UploadStateResult] = {
    val keys  = backedUpData.asScala.map { case (k, _) => k }.toVector.distinct.sortBy(Utils.keyToOffsetDateTime)
    val index = keys.indexOf(key)

    val uploadStateResult =
      if (index < 0)
        UploadStateResult.empty
      else {
        val previous =
          keys.lift(index - 1).map(k => PreviousState(StateDetails((), BackupObjectMetadata.fromKey(k)), k))
        val current = Some(StateDetails((), BackupObjectMetadata.fromKey(key)))
        UploadStateResult(current, previous)
      }

    Future.successful(uploadStateResult)
  }

  override def empty: () => Future[Done] = () => Future.successful(Done)

  override def backupToStorageTerminateSink(previousState: PreviousState): Sink[ByteString, Future[Done]] =
    Sink.foreach[ByteString] { byteString =>
      backedUpData.add((previousState.previousKey, byteString))
    }

  override def backupToStorageSink(key: String,
                                   currentState: Option[Unit]
  ): Sink[(ByteString, kafkaClientInterface.CursorContext), Future[Done]] =
    Flow[(ByteString, kafkaClientInterface.CursorContext)]
      .alsoTo(kafkaClientInterface.commitCursor.contramap[(ByteString, kafkaClientInterface.CursorContext)] {
        case (_, cursor) => cursor
      })
      .to(Sink.foreach { case (byteString, _) =>
        backedUpData.add((key, byteString))
      })
      .mapMaterializedValue(_ => Future.successful(Done))

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

  def decompress(byteString: ByteString, compression: Option[CompressionType])(implicit
      system: ActorSystem
  ): ByteString =
    compression match {
      case Some(Gzip) =>
        Await.result(Source.single(byteString).via(Compression.gunzip()).runFold(ByteString.empty)(_ ++ _),
                     Duration.Inf
        )
      case None => byteString
    }

  def joinByteString(byteStrings: IterableOnce[ByteString]): ByteString =
    byteStrings.iterator.foldLeft(ByteString.empty)(_ ++ _)
}

/** A `MockedBackupClientInterface` that also uses a mocked `KafkaClientInterface`
  */
class MockedBackupClientInterfaceWithMockedKafkaData(
    kafkaData: Source[ReducedConsumerRecord, NotUsed],
    timeConfiguration: TimeConfiguration,
    compression: Option[CompressionModel] = None,
    commitStorage: ConcurrentLinkedDeque[Long] = new ConcurrentLinkedDeque[Long](),
    backedUpData: ConcurrentLinkedQueue[(String, ByteString)] = new ConcurrentLinkedQueue[(String, ByteString)](),
    stopAfterDuration: Option[FiniteDuration] = None,
    handleOffsets: Boolean = false
)(implicit override val system: ActorSystem)
    extends MockedBackupClientInterface(
      new MockedKafkaConsumerInterface(kafkaData, commitStorage, stopAfterDuration, handleOffsets),
      timeConfiguration,
      compression,
      backedUpData
    )
