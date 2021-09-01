package io.aiven.guardian.kafka.backup

import akka.actor.ActorSystem
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffMatcher._
import io.aiven.guardian.akka.AkkaStreamTestKit
import io.aiven.guardian.akka.AnyPropTestKit
import io.aiven.guardian.kafka.Generators.KafkaDataWithTimePeriod
import io.aiven.guardian.kafka.Generators.kafkaDataWithTimePeriodsGen
import io.aiven.guardian.kafka.ScalaTestConstants
import io.aiven.guardian.kafka.codecs.Circe._
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import org.mdedetrich.akka.stream.support.CirceStreamSupport
import org.scalatest.Inspectors
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import java.time.temporal.ChronoUnit

final case class Periods(periodsBefore: Long, periodsAfter: Long)

class BackupClientInterfaceSpec
    extends AnyPropTestKit(ActorSystem("BackupClientInterfaceSpec"))
    with AkkaStreamTestKit
    with Matchers
    with ScalaCheckPropertyChecks
    with ScalaTestConstants {

  property("Ordered Kafka events should produce at least one BackupStreamPosition.Boundary") {
    forAll(kafkaDataWithTimePeriodsGen()) { (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod) =>
      val mock = new MockedBackupClientInterfaceWithMockedKafkaData(kafkaDataWithTimePeriod.data,
                                                                    kafkaDataWithTimePeriod.periodSlice
      )

      val calculatedFuture = mock.materializeBackupStreamPositions()

      val result = Await.result(calculatedFuture, AwaitTimeout).toList
      val backupStreamPositions = result.map { case (_, backupStreamPosition) =>
        backupStreamPosition
      }

      Inspectors.forAtLeast(1, backupStreamPositions)(
        _ must matchTo(BackupStreamPosition.Boundary: BackupStreamPosition)
      )
    }
  }

  property(
    "Every ReducedConsumerRecord after a BackupStreamPosition.Boundary should be in the next consecutive time period"
  ) {
    forAll(kafkaDataWithTimePeriodsGen()) { (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod) =>
      val mock = new MockedBackupClientInterfaceWithMockedKafkaData(kafkaDataWithTimePeriod.data,
                                                                    kafkaDataWithTimePeriod.periodSlice
      )

      val calculatedFuture = mock.materializeBackupStreamPositions()

      val result = Await.result(calculatedFuture, AwaitTimeout).toList

      val allBoundariesWithoutMiddles = result
        .sliding(2)
        .collect { case Seq((_, _: BackupStreamPosition.Boundary.type), (afterRecord, _)) =>
          afterRecord
        }
        .toList

      if (allBoundariesWithoutMiddles.length > 1) {
        @nowarn("msg=not.*?exhaustive")
        val withBeforeAndAfter =
          allBoundariesWithoutMiddles.sliding(2).map { case Seq(before, after) => (before, after) }.toList

        val initialTime = kafkaDataWithTimePeriod.data.head.timestamp

        Inspectors.forEvery(withBeforeAndAfter) { case (before, after) =>
          val periodAsMillis = kafkaDataWithTimePeriod.periodSlice.toMillis
          ((before.timestamp - initialTime) / periodAsMillis) mustNot equal(
            (after.timestamp - initialTime) / periodAsMillis
          )
        }
      }
    }
  }

  property(
    "The time difference between two consecutive BackupStreamPosition.Middle's has to be less then the specified time period"
  ) {
    forAll(kafkaDataWithTimePeriodsGen()) { (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod) =>
      val mock = new MockedBackupClientInterfaceWithMockedKafkaData(kafkaDataWithTimePeriod.data,
                                                                    kafkaDataWithTimePeriod.periodSlice
      )

      val calculatedFuture = mock.materializeBackupStreamPositions()

      val result = Await.result(calculatedFuture, AwaitTimeout).toList

      val allCoupledMiddles = result
        .sliding(2)
        .collect {
          case Seq((beforeRecord, _: BackupStreamPosition.Middle.type),
                   (afterRecord, _: BackupStreamPosition.Middle.type)
              ) =>
            (beforeRecord, afterRecord)
        }
        .toList

      Inspectors.forEvery(allCoupledMiddles) { case (before, after) =>
        ChronoUnit.MICROS.between(before.toOffsetDateTime,
                                  after.toOffsetDateTime
        ) must be < kafkaDataWithTimePeriod.periodSlice.toMicros
      }
    }
  }

  property("backup method completes flow correctly for all valid Kafka events") {
    forAll(kafkaDataWithTimePeriodsGen()) { (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod) =>
      val mock = new MockedBackupClientInterfaceWithMockedKafkaData(kafkaDataWithTimePeriod.data,
                                                                    kafkaDataWithTimePeriod.periodSlice
      )

      implicit val ec = ExecutionContext.global
      val calculatedFuture = for {
        _ <- mock.backup.run()
        _ <- akka.pattern.after(100 millis)(Future.successful(()))
        processedRecords = mock.mergeBackedUpData
        asRecords <- Future.sequence(processedRecords.map { case (key, byteString) =>
                       Source
                         .single(byteString)
                         .via(CirceStreamSupport.decode[List[ReducedConsumerRecord]])
                         .toMat(Sink.collection)(Keep.right)
                         .run()
                         .map(records => (key, records.flatten))
                     })
      } yield asRecords

      val result = Await.result(calculatedFuture, AwaitTimeout)

      val observed = result.flatMap { case (_, values) => values }

      kafkaDataWithTimePeriod.data.containsSlice(observed) mustEqual true
      if (observed.nonEmpty) {
        observed.head must matchTo(kafkaDataWithTimePeriod.data.head)
      }
    }
  }
}
