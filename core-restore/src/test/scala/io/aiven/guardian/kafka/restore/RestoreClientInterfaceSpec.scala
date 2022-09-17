package io.aiven.guardian.kafka.restore

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffMustMatcher._
import io.aiven.guardian.akka.AkkaStreamTestKit
import io.aiven.guardian.akka.AnyPropTestKit
import io.aiven.guardian.kafka.ExtensionsMethods._
import io.aiven.guardian.kafka.Generators._
import io.aiven.guardian.kafka.Utils
import io.aiven.guardian.kafka.backup.MockedBackupClientInterfaceWithMockedKafkaData
import io.aiven.guardian.kafka.backup.configs.PeriodFromFirst
import io.aiven.guardian.kafka.configs.{KafkaCluster => KafkaClusterConfig}
import io.aiven.guardian.kafka.restore.configs.{Restore => RestoreConfig}
import org.scalatest.Inspectors
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.language.postfixOps

import java.time.temporal.ChronoUnit

class RestoreClientInterfaceSpec
    extends AnyPropTestKit(ActorSystem("RestoreClientInterfaceSpec"))
    with AkkaStreamTestKit
    with Matchers
    with ScalaFutures
    with ScalaCheckPropertyChecks {

  implicit val ec: ExecutionContext            = system.dispatcher
  implicit val defaultPatience: PatienceConfig = PatienceConfig(90 seconds, 100 millis)

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
                                                           )
        )

      backupMock.clear()
      val calculatedFuture = for {
        _ <- backupMock.backup.run()
        _ <- akka.pattern.after(AkkaStreamInitializationConstant)(Future.successful(()))
        processedRecords = backupMock.mergeBackedUpData()
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
                                                           PeriodFromFirst(kafkaDataWithTimePeriod.periodSlice)
        )

      backupMock.clear()
      val calculatedFuture = for {
        _ <- backupMock.backup.run()
        _ <- akka.pattern.after(AkkaStreamInitializationConstant)(Future.successful(()))
        processedRecords = backupMock.mergeBackedUpData()
        restoreMock      = new MockedRestoreClientInterface(processedRecords.toMap)
        _ <- restoreMock.restore
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
                                                           )
        )

      backupMock.clear()
      val calculatedFuture = for {
        _ <- backupMock.backup.run()
        _ <- akka.pattern.after(AkkaStreamInitializationConstant)(Future.successful(()))
        processedRecords = backupMock.mergeBackedUpData()
        restoreMock      = new MockedRestoreClientInterface(processedRecords.toMap)
        _ <- restoreMock.restore
        restoredData = restoreMock.kafkaProducerInterface.producedData.asScala.toList
      } yield restoredData

      calculatedFuture.futureValue mustMatchTo kafkaDataWithTimePeriodAndPickedRecord.data.filter {
        reducedConsumerRecord =>
          reducedConsumerRecord.toOffsetDateTime >= kafkaDataWithTimePeriodAndPickedRecord.picked.toOffsetDateTime
      }
    }

  }
}
