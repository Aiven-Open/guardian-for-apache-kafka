package io.aiven.guardian.kafka.backup.s3

import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffMustMatcher._
import com.typesafe.scalalogging.LazyLogging
import io.aiven.guardian.kafka.Generators._
import io.aiven.guardian.kafka.backup.configs.PeriodFromFirst
import io.aiven.guardian.kafka.codecs.Circe._
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.kafka.s3.Generators._
import io.aiven.guardian.kafka.s3.S3Spec
import io.aiven.guardian.kafka.s3.configs.{S3 => S3Config}
import org.mdedetrich.akka.stream.support.CirceStreamSupport
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.must.Matchers

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import java.time.OffsetDateTime

trait BackupClientSpec extends S3Spec with Matchers with BeforeAndAfterAll with LazyLogging {

  val ThrottleElements: Int          = 100
  val ThrottleAmount: FiniteDuration = 1 millis

  property("backup method completes flow correctly for all valid Kafka events", RealS3Available) {
    forAll(kafkaDataWithTimePeriodsGen(), s3ConfigGen(useVirtualDotHost, bucketPrefix)) {
      (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod, s3Config: S3Config) =>
        logger.info(s"Data bucket is ${s3Config.dataBucket}")
        val backupClient = new MockedS3BackupClientInterface(
          Source(kafkaDataWithTimePeriod.data).throttle(ThrottleElements, ThrottleAmount),
          PeriodFromFirst(kafkaDataWithTimePeriod.periodSlice),
          s3Config,
          Some(s3Settings)
        )

        val delay =
          (ThrottleAmount * (kafkaDataWithTimePeriod.data.size / ThrottleElements) * 1.2) + (10 millis) match {
            case fd: FiniteDuration   => fd
            case _: Duration.Infinite => throw new Exception("Expected Finite Duration")
          }

        val calculatedFuture = for {
          _ <- createBucket(s3Config.dataBucket)
          _ <- backupClient.backup.run()
          _ <- akka.pattern.after(delay)(Future.successful(()))
          bucketContents <-
            S3.listBucket(s3Config.dataBucket, None, s3Headers)
              .withAttributes(s3Attrs)
              .toMat(Sink.collection)(Keep.right)
              .run()
          keysWithRecords <- Future.sequence(bucketContents.map { bucketContents =>
                               S3.getObject(s3Config.dataBucket, bucketContents.key)
                                 .withAttributes(s3Attrs)
                                 .via(CirceStreamSupport.decode[List[Option[ReducedConsumerRecord]]])
                                 .toMat(Sink.collection)(Keep.right)
                                 .run()
                                 .map(list => (bucketContents.key, list.flatten))(ExecutionContext.parasitic)
                             })
          sorted = keysWithRecords.toList.sortBy { case (key, _) =>
                     val date = key.replace(".json", "")
                     OffsetDateTime.parse(date).toEpochSecond
                   }(Ordering[Long].reverse)
          flattened = sorted.flatMap { case (_, records) => records }
        } yield flattened.collect { case Some(reducedConsumerRecord) =>
          reducedConsumerRecord
        }
        val observed = calculatedFuture.futureValue

        kafkaDataWithTimePeriod.data.containsSlice(observed) mustEqual true
        if (observed.nonEmpty) {
          observed.head mustMatchTo (kafkaDataWithTimePeriod.data.head)
        }
    }
  }
}
