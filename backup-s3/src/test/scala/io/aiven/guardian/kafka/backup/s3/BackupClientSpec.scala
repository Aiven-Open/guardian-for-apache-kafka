package io.aiven.guardian.kafka.backup.s3

import akka.actor.ActorSystem
import akka.stream.Attributes
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.alpakka.s3.{AccessStyle, S3Attributes, S3Settings}
import akka.stream.scaladsl.{Keep, Sink}
import com.adobe.testing.s3mock.S3MockApplication
import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffMatcher.matchTo
import io.aiven.guardian.kafka.backup.{KafkaDataWithTimePeriod, Periods}
import io.aiven.guardian.kafka.codecs.Circe._
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.kafka.s3.Config
import io.aiven.guardian.kafka.s3.Generators._
import io.aiven.guardian.kafka.s3.configs.{S3 => S3Config}
import io.aiven.guardian.kafka.s3.errors.S3Errors
import io.aiven.guardian.kafka.{Generators, ScalaTestConstants}
import org.mdedetrich.akka.stream.support.CirceStreamSupport
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.must.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.AwsRegionProvider

import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

class BackupClientSpec
    extends AnyPropSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with ScalaTestConstants
    with BeforeAndAfterAll
    with Config {

  val s3MockApplication = new AtomicReference[S3MockApplication]()
  val s3Settings        = new AtomicReference[S3Settings]()

  implicit val system: ActorSystem = ActorSystem()

  override def beforeAll(): Unit = {
    val mock = S3MockApplication.start(
      Map[String, AnyRef](
        S3MockApplication.PROP_HTTP_PORT         -> (S3MockApplication.RANDOM_PORT: Integer),
        S3MockApplication.PROP_SECURE_CONNECTION -> (false: java.lang.Boolean)
      ).asJava
    )

    val endpoint = s"http://localhost:${mock.getHttpPort}"

    val settings = S3Settings()
      .withEndpointUrl(endpoint)
      .withS3RegionProvider(new AwsRegionProvider {
        override def getRegion: Region = Region.EU_NORTH_1
      })
      .withCredentialsProvider(() => AwsBasicCredentials.create("foo", "bar"))
      .withAccessStyle(AccessStyle.PathAccessStyle)
    s3MockApplication.set(mock)
    s3Settings.set(settings)
  }

  override def afterAll(): Unit =
    s3MockApplication.get().stop()

  val periodGen = for {
    before <- Gen.long
    after  <- Gen.long
  } yield Periods(before, after)

  val s3ConfigGen = (for {
    dataBucket       <- bucketNameGen
    compactionBucket <- bucketNameGen
  } yield S3Config(dataBucket, compactionBucket)).filter(config => config.dataBucket != config.compactionBucket)

  def kafkaDataWithTimePeriodsGen: Gen[KafkaDataWithTimePeriod] = for {
    topic   <- Gen.alphaStr
    records <- Generators.kafkaReducedConsumerRecordsGen(topic, 2, 100, 10)
    head = records.head
    last = records.last

    duration <- Gen.choose[Long](head.timestamp, last.timestamp - 1).map(millis => FiniteDuration(millis, MILLISECONDS))
  } yield KafkaDataWithTimePeriod(records, duration)

  property("backup method completes flow correctly for all valid Kafka events") {
    forAll(kafkaDataWithTimePeriodsGen, s3ConfigGen) {
      val settings = s3Settings.get()
      (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod, s3Config: S3Config) =>
        val backupClient = new MockedS3BackupClientInterface(kafkaDataWithTimePeriod.data,
                                                             kafkaDataWithTimePeriod.periodSlice,
                                                             s3Config,
                                                             Some(settings)
        )

        implicit val ec: ExecutionContext = ExecutionContext.global
        implicit val s3Attrs: Attributes  = S3Attributes.settings(settings)

        val calculatedFuture = for {
          _ <- S3.makeBucket(s3Config.dataBucket)
          _ <- backupClient.backup.run()
          _ <- akka.pattern.after(1 second)(Future.successful(()))
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
