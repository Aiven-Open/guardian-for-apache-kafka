package aiven.io.guardian.kafka.backup

import aiven.io.guardian.kafka.Generators
import aiven.io.guardian.kafka.models.ReducedConsumerRecord
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink}
import org.scalacheck.Gen
import org.scalatest.Inspectors
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.temporal.ChronoUnit
import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

final case class Periods(periodsBefore: Long, periodsAfter: Long)

final case class KafkaDataWithTimePeriod(data: List[ReducedConsumerRecord], periodSlice: FiniteDuration)

class BackupClientInterfaceSpec extends AnyWordSpec with Matchers with ScalaCheckPropertyChecks {

  implicit val system: ActorSystem = ActorSystem()

  val periodGen = for {
    before <- Gen.long
    after  <- Gen.long
  } yield Periods(before, after)

  def kafkaDataWithTimePeriodsGen: Gen[KafkaDataWithTimePeriod] = for {
    topic   <- Gen.alphaStr
    records <- Generators.kafkaReducedConsumerRecordsGen(topic, 2, 100, 10)
    head = records.head
    last = records.last

    duration <- Gen.choose[Long](head.timestamp, last.timestamp - 1).map(millis => FiniteDuration(millis, MILLISECONDS))
  } yield KafkaDataWithTimePeriod(records, duration)

  "BackupClientInterface" can {
    "splitAtBoundaryCondition" should {
      "BackupStreamPosition.Boundary happy case" in {
        forAll(periodGen.filter(x => x.periodsAfter > x.periodsBefore)) { (periods: Periods) =>
          BackupClientInterface.splitAtBoundaryCondition(periods.periodsBefore,
                                                         periods.periodsAfter
          ) mustEqual BackupStreamPosition.Boundary
        }
      }

      "BackupStreamPosition.Middle happy case" in {
        forAll(periodGen.filter(x => !(x.periodsAfter > x.periodsBefore))) { (periods: Periods) =>
          BackupClientInterface.splitAtBoundaryCondition(periods.periodsBefore,
                                                         periods.periodsAfter
          ) mustEqual BackupStreamPosition.Middle
        }
      }
    }

    "calculateBackupStreamPositions" should {
      "calculateBackupStreamPositions happy case" in {
        forAll(kafkaDataWithTimePeriodsGen) { (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod) =>
          val mock = new MockedBackupClientInterfaceWithMockedKafkaData(kafkaDataWithTimePeriod.data,
                                                                        kafkaDataWithTimePeriod.periodSlice
          )

          val calculatedFuture = mock
            .calculateBackupStreamPositions(mock.sourceWithPeriods)
            .asSource
            .map { case (data, _) =>
              data
            }
            .toMat(Sink.collection)(Keep.right)
            .run()

          val result = Await.result(calculatedFuture, 10 minutes).toList
          val backupStreamPositions = result.map { case (_, backupStreamPosition) =>
            backupStreamPosition
          }

          // We must always have at least one Boundary, a single Middle makes no sense
          Inspectors.forAtLeast(1, backupStreamPositions)(_ mustEqual BackupStreamPosition.Boundary)

          // If you look for each event after a BackupStreamPosition.Boundary then it must always be in the next
          // consecutive time period from beginning of the stream
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

          val allCoupledMiddles = result
            .sliding(2)
            .collect {
              case Seq((beforeRecord, _: BackupStreamPosition.Middle.type),
                       (afterRecord, _: BackupStreamPosition.Middle.type)
                  ) =>
                (beforeRecord, afterRecord)
            }
            .toList

          // If you have 2 BackupStreamPosition.Middle next to eachother than the time between them always
          // has to be less then the period
          Inspectors.forEvery(allCoupledMiddles) { case (before, after) =>
            ChronoUnit.MICROS.between(before.toOffsetDateTime,
                                      after.toOffsetDateTime
            ) must be < kafkaDataWithTimePeriod.periodSlice.toMicros
          }
        }
      }
    }
  }
}
