package io.aiven.guardian.kafka.restore

import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffMustMatcher._
import com.typesafe.scalalogging.StrictLogging
import io.aiven.guardian.kafka.ExtensionsMethods._
import io.aiven.guardian.kafka.Generators._
import io.aiven.guardian.kafka.Utils
import io.aiven.guardian.kafka.backup.MockedBackupClientInterfaceWithMockedKafkaData
import io.aiven.guardian.kafka.backup.configs.Compression
import io.aiven.guardian.kafka.backup.configs.PeriodFromFirst
import io.aiven.guardian.kafka.configs.{KafkaCluster => KafkaClusterConfig}
import io.aiven.guardian.kafka.restore.configs.{Restore => RestoreConfig}
import io.aiven.guardian.pekko.PekkoStreamTestKit
import org.apache.pekko
import org.scalatest.Inspectors
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatest.propspec.AnyPropSpecLike
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.language.postfixOps

import java.time.temporal.ChronoUnit

import pekko.stream.scaladsl.Source

trait RestoreClientInterfaceTest
    extends AnyPropSpecLike
    with PekkoStreamTestKit
    with Matchers
    with ScalaFutures
    with ScalaCheckPropertyChecks
    with StrictLogging {

  implicit val ec: ExecutionContext            = system.dispatcher
  implicit val defaultPatience: PatienceConfig = PatienceConfig(90 seconds, 100 millis)

  def compression: Option[Compression]

  property("Calculating finalKeys contains correct keys with fromWhen filter") {
    forAll(
      kafkaDataWithTimePeriodsAndPickedRecordGen(padTimestampsMillis =
                                                   Range.inclusive(1, ChronoUnit.HOURS.getDuration.toMillis.toInt),
                                                 min = 1000,
                                                 max = 1000
      )
    ) { (kafkaDataWithTimePeriodAndPickedRecord: KafkaDataWithTimePeriodAndPickedRecord) =>
      implicit val mockedKafkaProducerInterface: MockedKafkaProducerInterface = new MockedKafkaProducerInterface
      implicit val kafkaClusterConfig: KafkaClusterConfig =
        KafkaClusterConfig(kafkaDataWithTimePeriodAndPickedRecord.topics)
      val pickedTime = kafkaDataWithTimePeriodAndPickedRecord.picked.toOffsetDateTime
      implicit val restoreConfig: RestoreConfig =
        RestoreConfig(Some(pickedTime), None)

      val backupMock =
        new MockedBackupClientInterfaceWithMockedKafkaData(Source(kafkaDataWithTimePeriodAndPickedRecord.data),
                                                           PeriodFromFirst(
                                                             ChronoUnit.SECONDS.getDuration.toScala
                                                           ),
                                                           compression
        )

      backupMock.clear()
      val calculatedFuture = for {
        _ <- backupMock.backup.run()
        _ <- pekko.pattern.after(PekkoStreamInitializationConstant)(Future.successful(()))
        processedRecords = backupMock.mergeBackedUpData(compression = compression.map(_.`type`))
        restoreMock      = new MockedRestoreClientInterface(processedRecords.toMap)
        keys <- restoreMock.finalKeys
      } yield keys

      val result = calculatedFuture.futureValue

      Inspectors.forEvery(result.drop(1)) { key =>
        (Utils.keyToOffsetDateTime(key) >= pickedTime) must be(true)
      }

    }
  }

  property("Round-trip with backup and restore") {
    forAll(kafkaDataWithTimePeriodsGen()) { (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod) =>
      implicit val restoreConfig: RestoreConfig                               = RestoreConfig.empty
      implicit val mockedKafkaProducerInterface: MockedKafkaProducerInterface = new MockedKafkaProducerInterface
      implicit val kafkaClusterConfig: KafkaClusterConfig = KafkaClusterConfig(kafkaDataWithTimePeriod.topics)

      val backupMock =
        new MockedBackupClientInterfaceWithMockedKafkaData(Source(kafkaDataWithTimePeriod.data),
                                                           PeriodFromFirst(kafkaDataWithTimePeriod.periodSlice),
                                                           compression
        )

      backupMock.clear()
      val calculatedFuture = for {
        _ <- backupMock.backup.run()
        _ <- pekko.pattern.after(10 seconds)(Future.successful(()))
        processedRecords = backupMock.mergeBackedUpData()
        restoreMock      = new MockedRestoreClientInterface(processedRecords.toMap)
        (_, runFuture)   = restoreMock.restore.run()
        _ <- runFuture
        restoredData = restoreMock.kafkaProducerInterface.producedData.asScala.toList
      } yield restoredData

      calculatedFuture.futureValue mustMatchTo kafkaDataWithTimePeriod.data
    }
  }

  property("Round-trip with backup and restore works using fromWhen filter") {
    forAll(
      kafkaDataWithTimePeriodsAndPickedRecordGen(padTimestampsMillis =
                                                   Range.inclusive(1, ChronoUnit.HOURS.getDuration.toMillis.toInt),
                                                 min = 1000,
                                                 max = 1000
      )
    ) { (kafkaDataWithTimePeriodAndPickedRecord: KafkaDataWithTimePeriodAndPickedRecord) =>
      implicit val mockedKafkaProducerInterface: MockedKafkaProducerInterface = new MockedKafkaProducerInterface
      implicit val kafkaClusterConfig: KafkaClusterConfig =
        KafkaClusterConfig(kafkaDataWithTimePeriodAndPickedRecord.topics)
      implicit val restoreConfig: RestoreConfig =
        RestoreConfig(Some(kafkaDataWithTimePeriodAndPickedRecord.picked.toOffsetDateTime), None)

      val backupMock =
        new MockedBackupClientInterfaceWithMockedKafkaData(Source(kafkaDataWithTimePeriodAndPickedRecord.data),
                                                           PeriodFromFirst(
                                                             kafkaDataWithTimePeriodAndPickedRecord.periodSlice
                                                           ),
                                                           compression
        )

      backupMock.clear()
      val calculatedFuture = for {
        _ <- backupMock.backup.run()
        _ <- pekko.pattern.after(PekkoStreamInitializationConstant)(Future.successful(()))
        processedRecords = backupMock.mergeBackedUpData()
        restoreMock      = new MockedRestoreClientInterface(processedRecords.toMap)
        (_, runFuture)   = restoreMock.restore.run()
        _ <- runFuture
        restoredData = restoreMock.kafkaProducerInterface.producedData.asScala.toList
      } yield restoredData

      calculatedFuture.futureValue mustMatchTo kafkaDataWithTimePeriodAndPickedRecord.data.filter {
        reducedConsumerRecord =>
          reducedConsumerRecord.toOffsetDateTime >= kafkaDataWithTimePeriodAndPickedRecord.picked.toOffsetDateTime
      }
    }

  }
}
