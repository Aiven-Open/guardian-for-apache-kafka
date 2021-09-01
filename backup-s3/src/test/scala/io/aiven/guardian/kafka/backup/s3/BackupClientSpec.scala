package io.aiven.guardian.kafka.backup.s3

import java.time.OffsetDateTime

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor.ActorSystem
import akka.stream.Attributes
import akka.stream.alpakka.s3.S3Attributes
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffMatcher.matchTo
import io.aiven.guardian.akka.AkkaHttpTestKit
import io.aiven.guardian.akka.AnyPropTestKit
import io.aiven.guardian.kafka.Generators._
import io.aiven.guardian.kafka.ScalaTestConstants
import io.aiven.guardian.kafka.codecs.Circe._
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.kafka.s3.Config
import io.aiven.guardian.kafka.s3.Generators._
import io.aiven.guardian.kafka.s3.MinioS3Test
import io.aiven.guardian.kafka.s3.configs.{S3 => S3Config}
import io.aiven.guardian.kafka.s3.errors.S3Errors
import org.mdedetrich.akka.stream.support.CirceStreamSupport
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BackupClientSpec
    extends AnyPropTestKit(ActorSystem("S3BackupClientSpec"))
    with AkkaHttpTestKit
    with Matchers
    with ScalaCheckPropertyChecks
    with ScalaTestConstants
    with MinioS3Test
    with Config {

  val ThrottleElements: Int          = 100
  val ThrottleAmount: FiniteDuration = 1 millis

  property("backup method completes flow correctly for all valid Kafka events") {
    forAll(kafkaDataWithTimePeriodsGen(), s3ConfigGen) {
      (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod, s3Config: S3Config) =>
        val backupClient = new MockedS3BackupClientInterface(kafkaDataWithTimePeriod.data,
                                                             kafkaDataWithTimePeriod.periodSlice,
                                                             s3Config,
                                                             Some(s3Settings),
                                                             Some(_.throttle(ThrottleElements, ThrottleAmount))
        )

        implicit val ec: ExecutionContext = ExecutionContext.global
        implicit val s3Attrs: Attributes  = S3Attributes.settings(s3Settings)

        val delay =
          (ThrottleAmount * (kafkaDataWithTimePeriod.data.size / ThrottleElements) * 1.2) + (10 millis) match {
            case fd: FiniteDuration   => fd
            case _: Duration.Infinite => throw new Exception("Expected Finite Duration")
          }

        val calculatedFuture = for {
          _ <- S3.makeBucket(s3Config.dataBucket)
          _ <- backupClient.backup.run()
          _ <- akka.pattern.after(delay)(Future.successful(()))
          bucketContents <-
            S3.listBucket(s3Config.dataBucket, None, s3Headers)
              .withAttributes(s3Attrs)
              .toMat(Sink.collection)(Keep.right)
              .run()
          keysWithSource <-
            Future.sequence(bucketContents.map { bucketContents =>
              S3.download(s3Config.dataBucket, bucketContents.key)
                .withAttributes(s3Attrs)
                .map(
                  _.getOrElse(
                    throw S3Errors
                      .ExpectedObjectToExist(s3Config.dataBucket, bucketContents.key, None, None, s3Headers)
                  )
                )
                .runWith(Sink.head)
                .map { case (source, _) =>
                  (bucketContents.key, source)
                }
            })
          keysWithRecords <- Future.sequence(keysWithSource.map { case (key, source) =>
                               source
                                 .via(CirceStreamSupport.decode[List[ReducedConsumerRecord]])
                                 .toMat(Sink.collection)(Keep.right)
                                 .run()
                                 .map(list => (key, list.flatten))
                             })
          sorted = keysWithRecords.toList.sortBy { case (key, _) =>
                     val date = key.replace(".json", "")
                     OffsetDateTime.parse(date).toEpochSecond
                   }(Ordering[Long].reverse)
          flattened = sorted.flatMap { case (_, records) => records }
        } yield flattened
        val observed = Await.result(calculatedFuture, AwaitTimeout)

        kafkaDataWithTimePeriod.data.containsSlice(observed) mustEqual true
        if (observed.nonEmpty) {
          observed.head must matchTo(kafkaDataWithTimePeriod.data.head)
        }
    }
  }
}
