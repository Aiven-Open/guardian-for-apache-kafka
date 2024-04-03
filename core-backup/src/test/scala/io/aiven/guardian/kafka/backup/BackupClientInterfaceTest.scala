package io.aiven.guardian.kafka.backup

import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffMustMatcher._
import io.aiven.guardian.kafka.Generators.KafkaDataWithTimePeriod
import io.aiven.guardian.kafka.Generators.kafkaDataWithTimePeriodsGen
import io.aiven.guardian.kafka.TestUtils.waitForStartOfTimeUnit
import io.aiven.guardian.kafka.backup.configs.ChronoUnitSlice
import io.aiven.guardian.kafka.backup.configs.PeriodFromFirst
import io.aiven.guardian.kafka.backup.configs.{Compression => CompressionModel}
import io.aiven.guardian.kafka.codecs.Circe._
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.pekko.PekkoStreamTestKit
import org.apache.kafka.common.record.TimestampType
import org.apache.pekko
import org.mdedetrich.pekko.stream.support.CirceStreamSupport
import org.scalatest.Inspectors
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatest.propspec.FixtureAnyPropSpecLike
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.typelevel.jawn.AsyncParser

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentLinkedQueue

import pekko.stream.scaladsl.Keep
import pekko.stream.scaladsl.Sink
import pekko.stream.scaladsl.Source
import pekko.util.ByteString

final case class Periods(periodsBefore: Long, periodsAfter: Long)

trait BackupClientInterfaceTest
    extends FixtureAnyPropSpecLike
    with PekkoStreamTestKit
    with Matchers
    with ScalaFutures
    with ScalaCheckPropertyChecks {

  implicit val ec: ExecutionContext            = system.dispatcher
  implicit val defaultPatience: PatienceConfig = PatienceConfig(90 seconds, 100 millis)

  def compression: Option[CompressionModel]

  property("Ordered Kafka events should produce at least one BackupStreamPosition.Boundary") { _ =>
    forAll(kafkaDataWithTimePeriodsGen()) { (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod) =>
      val mock =
        new MockedBackupClientInterfaceWithMockedKafkaData(Source(kafkaDataWithTimePeriod.data),
                                                           PeriodFromFirst(kafkaDataWithTimePeriod.periodSlice)
        )

      val calculatedFuture = mock.materializeBackupStreamPositions()

      Inspectors.forAtLeast(1, calculatedFuture.futureValue)(
        _ must equal(mock.End: mock.RecordElement)
      )
    }
  }

  property(
    "Every ReducedConsumerRecord after a BackupStreamPosition.Boundary should be in the next consecutive time period"
  ) { _ =>
    forAll(kafkaDataWithTimePeriodsGen()) { (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod) =>
      val mock =
        new MockedBackupClientInterfaceWithMockedKafkaData(Source(kafkaDataWithTimePeriod.data),
                                                           PeriodFromFirst(kafkaDataWithTimePeriod.periodSlice)
        )

      val result = mock.materializeBackupStreamPositions().futureValue.toList

      val allBoundariesWithoutMiddles = result
        .sliding(2)
        .collect { case Seq(mock.End, afterRecordRecordElement: mock.Element) =>
          afterRecordRecordElement
        }
        .toList

      if (allBoundariesWithoutMiddles.length > 1) {
        @nowarn("msg=not.*?exhaustive")
        val withBeforeAndAfter =
          allBoundariesWithoutMiddles.sliding(2).map { case Seq(first, second) => (first, second) }.toList

        val initialTime = kafkaDataWithTimePeriod.data.head.timestamp

        Inspectors.forEvery(withBeforeAndAfter) { case (first, second) =>
          val periodAsMillis = kafkaDataWithTimePeriod.periodSlice.toMillis
          ((first.reducedConsumerRecord.timestamp - initialTime) / periodAsMillis) mustNot equal(
            (second.reducedConsumerRecord.timestamp - initialTime) / periodAsMillis
          )
        }
      }
    }
  }

  property(
    "The time difference between two consecutive BackupStreamPosition.Middle's has to be less then the specified time period"
  ) { _ =>
    forAll(kafkaDataWithTimePeriodsGen()) { (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod) =>
      val mock =
        new MockedBackupClientInterfaceWithMockedKafkaData(Source(kafkaDataWithTimePeriod.data),
                                                           PeriodFromFirst(kafkaDataWithTimePeriod.periodSlice)
        )

      val result = mock.materializeBackupStreamPositions().futureValue.toList

      val allCoupledMiddles = result
        .sliding(2)
        .collect { case Seq(first: mock.Element, second: mock.Element) =>
          (first, second)
        }
        .toList

      Inspectors.forEvery(allCoupledMiddles) { case (first, second) =>
        ChronoUnit.MICROS.between(first.reducedConsumerRecord.toOffsetDateTime,
                                  second.reducedConsumerRecord.toOffsetDateTime
        ) must be < kafkaDataWithTimePeriod.periodSlice.toMicros
      }
    }
  }

  property("the time difference between the first and last timestamp for a given key is less than time period") { _ =>
    forAll(kafkaDataWithTimePeriodsGen().filter(_.data.size > 2)) {
      (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod) =>
        val mock =
          new MockedBackupClientInterfaceWithMockedKafkaData(Source(kafkaDataWithTimePeriod.data),
                                                             PeriodFromFirst(kafkaDataWithTimePeriod.periodSlice)
          )

        mock.clear()
        val calculatedFuture = for {
          _ <- mock.backup.run()
          _ <- pekko.pattern.after(PekkoStreamInitializationConstant)(Future.successful(()))
          processedRecords = mock.mergeBackedUpData()
          asRecords <- Future.sequence(processedRecords.map { case (key, byteString) =>
                         Source
                           .single(byteString)
                           .via(CirceStreamSupport.decode[Option[ReducedConsumerRecord]](AsyncParser.UnwrapArray))
                           .collect { case Some(value) =>
                             value
                           }
                           .toMat(Sink.collection)(Keep.right)
                           .run()
                           .map(records => (key, records))
                       })
        } yield asRecords

        val result = calculatedFuture.futureValue

        Inspectors.forEvery(result) { case (_, records) =>
          (records.headOption, records.lastOption) match {
            case (Some(first), Some(last)) if first != last =>
              ChronoUnit.MICROS.between(first.toOffsetDateTime,
                                        last.toOffsetDateTime
              ) must be < kafkaDataWithTimePeriod.periodSlice.toMicros
            case _ =>
          }
        }
    }
  }

  property("backup method completes flow correctly for all valid Kafka events") { _ =>
    forAll(kafkaDataWithTimePeriodsGen().filter(_.data.size > 2)) {
      (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod) =>
        val mock =
          new MockedBackupClientInterfaceWithMockedKafkaData(Source(kafkaDataWithTimePeriod.data),
                                                             PeriodFromFirst(kafkaDataWithTimePeriod.periodSlice),
                                                             compression
          )

        mock.clear()
        val calculatedFuture = for {
          _ <- mock.backup.run()
          _ <- pekko.pattern.after(PekkoStreamInitializationConstant)(Future.successful(()))
          processedRecords = mock.mergeBackedUpData(compression = compression.map(_.`type`))
          asRecords <- Future.sequence(processedRecords.map { case (key, byteString) =>
                         Source
                           .single(byteString)
                           .via(CirceStreamSupport.decode[Option[ReducedConsumerRecord]](AsyncParser.UnwrapArray))
                           .collect { case Some(value) =>
                             value
                           }
                           .toMat(Sink.collection)(Keep.right)
                           .run()
                           .map(records => (key, records))
                       })
        } yield asRecords

        val result = calculatedFuture.futureValue

        val observed = result.flatMap { case (_, values) => values }

        kafkaDataWithTimePeriod.data mustEqual observed
    }
  }

  property("backup method completes flow correctly for single element") { _ =>
    val reducedConsumerRecord = ReducedConsumerRecord("", 0, 1, Some("key"), "value", 1, TimestampType.CREATE_TIME)

    val mock = new MockedBackupClientInterfaceWithMockedKafkaData(Source.single(
                                                                    reducedConsumerRecord
                                                                  ),
                                                                  PeriodFromFirst(1 day),
                                                                  compression
    )
    mock.clear()
    val calculatedFuture = for {
      _ <- mock.backup.run()
      _ <- pekko.pattern.after(PekkoStreamInitializationConstant)(Future.successful(()))
      processedRecords = mock.mergeBackedUpData(compression = compression.map(_.`type`))
      asRecords <- Future.sequence(processedRecords.map { case (key, byteString) =>
                     Source
                       .single(byteString)
                       .via(CirceStreamSupport.decode[Option[ReducedConsumerRecord]](AsyncParser.UnwrapArray))
                       .collect { case Some(value) =>
                         value
                       }
                       .toMat(Sink.collection)(Keep.right)
                       .run()
                       .map(records => (key, records))
                   })
    } yield asRecords

    val result = calculatedFuture.futureValue

    val observed = result.flatMap { case (_, values) => values }

    List(reducedConsumerRecord) mustEqual observed
  }

  property("backup method completes flow correctly for two elements") { _ =>
    val reducedConsumerRecords = List(
      ReducedConsumerRecord("", 0, 1, Some("key"), "value1", 1, TimestampType.CREATE_TIME),
      ReducedConsumerRecord("", 0, 2, Some("key"), "value2", 2, TimestampType.CREATE_TIME)
    )

    val mock = new MockedBackupClientInterfaceWithMockedKafkaData(Source(
                                                                    reducedConsumerRecords
                                                                  ),
                                                                  PeriodFromFirst(1 millis),
                                                                  compression
    )
    mock.clear()
    val calculatedFuture = for {
      _ <- mock.backup.run()
      _ <- pekko.pattern.after(PekkoStreamInitializationConstant)(Future.successful(()))
      processedRecords = mock.mergeBackedUpData(compression = compression.map(_.`type`))
      asRecords <- Future.sequence(processedRecords.map { case (key, byteString) =>
                     Source
                       .single(byteString)
                       .via(CirceStreamSupport.decode[Option[ReducedConsumerRecord]](AsyncParser.UnwrapArray))
                       .collect { case Some(value) =>
                         value
                       }
                       .toMat(Sink.collection)(Keep.right)
                       .run()
                       .map(records => (key, records))
                   })
    } yield asRecords

    val result = calculatedFuture.futureValue

    val observed = result.flatMap { case (_, values) => values }

    reducedConsumerRecords mustEqual observed
  }

  property("backup method correctly terminates every key apart from last") { _ =>
    forAll(kafkaDataWithTimePeriodsGen().filter(_.data.size > 1)) {
      (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod) =>
        val mock =
          new MockedBackupClientInterfaceWithMockedKafkaData(Source(kafkaDataWithTimePeriod.data),
                                                             PeriodFromFirst(kafkaDataWithTimePeriod.periodSlice),
                                                             compression
          )

        mock.clear()
        val calculatedFuture = for {
          _ <- mock.backup.run()
          _ <- pekko.pattern.after(PekkoStreamInitializationConstant)(Future.successful(()))
          processedRecords = mock.mergeBackedUpData(terminate = false, compression = compression.map(_.`type`))
        } yield processedRecords.splitAt(processedRecords.length - 1)

        val (terminated, nonTerminated) = calculatedFuture.futureValue

        if (nonTerminated.nonEmpty) {
          Inspectors.forEvery(terminated) { case (_, byteString) =>
            byteString.utf8String.takeRight(2) mustEqual "}]"
          }
        }

        Inspectors.forEvery(nonTerminated) { case (_, byteString) =>
          byteString.utf8String.takeRight(2) mustEqual "},"
        }
    }
  }

  property("suspend/resume for same object using ChronoUnitSlice works correctly") { _ =>
    // Since this test needs to wait for the start of the next minute we only want it to
    // succeed once otherwise it runs for a very long time.
    implicit val generatorDrivenConfig: PropertyCheckConfiguration =
      PropertyCheckConfiguration(minSuccessful = 1)

    forAll(kafkaDataWithTimePeriodsGen()) { (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod) =>
      val commitStorage = new ConcurrentLinkedDeque[Long]()
      val backupStorage = new ConcurrentLinkedQueue[(String, ByteString)]()
      val data          = kafkaDataWithTimePeriod.data

      val mockOne = new MockedBackupClientInterfaceWithMockedKafkaData(
        Source(data),
        ChronoUnitSlice(ChronoUnit.MINUTES),
        None,
        commitStorage,
        backupStorage,
        stopAfterDuration = Some(kafkaDataWithTimePeriod.periodSlice),
        handleOffsets = true
      )

      val mockTwo = new MockedBackupClientInterfaceWithMockedKafkaData(Source(data),
                                                                       ChronoUnitSlice(ChronoUnit.MINUTES),
                                                                       None,
                                                                       commitStorage,
                                                                       backupStorage,
                                                                       stopAfterDuration = None,
                                                                       handleOffsets = true
      )

      val calculatedFuture = for {
        _ <- waitForStartOfTimeUnit(ChronoUnit.MINUTES)
        _ <- mockOne.backup.run()
        _ <- pekko.pattern.after(PekkoStreamInitializationConstant)(mockTwo.backup.run())
        processedRecords <-
          pekko.pattern.after(PekkoStreamInitializationConstant)(
            Future.successful(
              mockTwo.mergeBackedUpData()
            )
          )
        asRecords <- Future.sequence(processedRecords.map { case (key, byteString) =>
                       Source
                         .single(byteString)
                         .via(CirceStreamSupport.decode[Option[ReducedConsumerRecord]](AsyncParser.UnwrapArray))
                         .collect { case Some(value) =>
                           value
                         }
                         .toMat(Sink.collection)(Keep.right)
                         .run()
                         .map(records => (key, records))
                     })
      } yield asRecords

      val result = calculatedFuture.futureValue

      val observed = result.flatMap { case (_, values) => values }

      data mustMatchTo observed
    }
  }
}
