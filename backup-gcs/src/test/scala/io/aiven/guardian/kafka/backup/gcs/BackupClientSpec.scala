package io.aiven.guardian.kafka.backup.gcs

import akka.actor.ActorSystem
import akka.stream.Attributes
import akka.stream.alpakka.googlecloud.storage.GCSAttributes
import akka.stream.alpakka.googlecloud.storage.scaladsl.GCStorage
import akka.stream.scaladsl.{Keep, Sink}
import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffMatcher.matchTo
import io.aiven.guardian.akka.{AkkaHttpTestKit, AnyPropTestKit}
import io.aiven.guardian.kafka.Generators._
import io.aiven.guardian.kafka.codecs.Circe._
import io.aiven.guardian.kafka.gcs.{Config, FakeGCSTest}
import io.aiven.guardian.kafka.gcs.Generators.gcsConfigGen
import io.aiven.guardian.kafka.gcs.configs.{GCS => GCSConfig}
import io.aiven.guardian.kafka.gcs.errors.GCSErrors
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import org.mdedetrich.akka.stream.support.CirceStreamSupport
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.OffsetDateTime
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

class BackupClientSpec
    extends AnyPropTestKit(ActorSystem("GCSBackupClientSpec"))
    with AkkaHttpTestKit
    with Matchers
    with ScalaCheckPropertyChecks
    with ScalaFutures
    with FakeGCSTest
    with Config {

  implicit val ec: ExecutionContext            = system.dispatcher
  implicit val defaultPatience: PatienceConfig = PatienceConfig(90 seconds, 100 millis)

  val ThrottleElements: Int          = 100
  val ThrottleAmount: FiniteDuration = 1 millis

  implicit lazy val gcsAttrs: Attributes = GCSAttributes.settings(gcsSettings)

  property("backup method completes flow correctly for all valid Kafka events") {
    forAll(kafkaDataWithTimePeriodsGen(), gcsConfigGen) {
      (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod, gcsConfig: GCSConfig) =>
        val backupClient = new MockedGCSBackupClientInterface(kafkaDataWithTimePeriod.data,
                                                              kafkaDataWithTimePeriod.periodSlice,
                                                              gcsConfig,
                                                              None,
                                                              Some(gcsSettings),
                                                              Some(_.throttle(ThrottleElements, ThrottleAmount))
        )

        val delay =
          (ThrottleAmount * (kafkaDataWithTimePeriod.data.size / ThrottleElements) * 1.2) + (10 millis) match {
            case fd: FiniteDuration   => fd
            case _: Duration.Infinite => throw new Exception("Expected Finite Duration")
          }

        val calculatedFuture = for {
          _ <- GCStorage.createBucket(gcsConfig.dataBucket, "EUROPE-WEST3")
          _ <- backupClient.backup.run()
          _ <- akka.pattern.after(delay)(Future.successful(()))
          bucketContents <-
            GCStorage
              .listBucket(gcsConfig.dataBucket, None)
              .withAttributes(gcsAttrs)
              .toMat(Sink.collection)(Keep.right)
              .run()
          keysWithSource <-
            Future.sequence(bucketContents.map { bucketContents =>
              GCStorage
                .download(gcsConfig.dataBucket, bucketContents.name)
                .withAttributes(gcsAttrs)
                .map(
                  _.getOrElse(
                    throw GCSErrors
                      .ExpectedObjectToExist(gcsConfig.dataBucket, bucketContents.name, None)
                  )
                )
                .runWith(Sink.head)
                .map { source =>
                  (bucketContents.name, source)
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
        val observed = calculatedFuture.futureValue

        kafkaDataWithTimePeriod.data.containsSlice(observed) mustEqual true
        if (observed.nonEmpty) {
          observed.head must matchTo(kafkaDataWithTimePeriod.data.head)
        }
    }
  }

}
