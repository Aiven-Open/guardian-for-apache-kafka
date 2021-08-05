package aiven.io.guardian.kafka.backup

import aiven.io.guardian.kafka.MockedKafkaClientInterface
import aiven.io.guardian.kafka.backup.configs.Backup
import aiven.io.guardian.kafka.models.ReducedConsumerRecord
import akka.Done
import akka.stream.scaladsl.Sink
import akka.util.ByteString

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/** A mocked `BackupClientInterface` which given a `kafkaClientInterface` allows you to
  * @param kafkaClientInterface
  * @param periodSlice
  */
class MockedBackupClientInterface(override val kafkaClientInterface: MockedKafkaClientInterface,
                                  periodSlice: FiniteDuration
) extends BackupClientInterface {

  /** A Map where the key object key/filename and the value is the contents of the currently streamed
    * `KafkaReducedRecord` data
    */
  val backedUpData = new ConcurrentHashMap[String, ByteString]().asScala

  override implicit lazy val backupConfig: Backup = Backup(
    periodSlice
  )

  /** Override this type to define the result of backing up data to a datasource
    */
  override type BackupResult = Done

  /** Override this method to define how to backup a `ByteString` to a `DataSource`
    *
    * @param key The object key or filename for what is being backed up
    * @return A Sink that also provides a `BackupResult`
    */
  override def backupToStorageSink(key: String): Sink[ByteString, Future[Done]] = Sink.foreach { byteString =>
    backedUpData.updateWith(key) {
      case Some(value) => Some(value ++ byteString)
      case None        => Some(byteString)
    }
  }
}

/** A `MockedBackupClientInterface` that also uses a mocked `KafkaClientInterface`
  */
class MockedBackupClientInterfaceWithMockedKafkaData(kafkaData: List[ReducedConsumerRecord],
                                                     periodSlice: FiniteDuration
) extends MockedBackupClientInterface(new MockedKafkaClientInterface(kafkaData), periodSlice)
