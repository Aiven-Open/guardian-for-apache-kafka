package io.aiven.guardian.kafka.backup.s3

import akka.stream.Attributes
import akka.stream.alpakka.s3.BucketAccess
import akka.stream.alpakka.s3.S3Attributes
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.testkit.TestKitBase
import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffMatcher.matchTo
import com.typesafe.scalalogging.StrictLogging
import io.aiven.guardian.akka.AkkaHttpTestKit
import io.aiven.guardian.kafka.Generators._
import io.aiven.guardian.kafka.codecs.Circe._
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.kafka.s3.Config
import io.aiven.guardian.kafka.s3.Generators._
import io.aiven.guardian.kafka.s3.configs.{S3 => S3Config}
import io.aiven.guardian.kafka.s3.errors.S3Errors
import org.mdedetrich.akka.stream.support.CirceStreamSupport
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatest.propspec.AnyPropSpecLike
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

import java.time.OffsetDateTime
import java.util.concurrent.ConcurrentLinkedQueue

trait BackupClientSpec
    extends TestKitBase
    with AnyPropSpecLike
    with AkkaHttpTestKit
    with Matchers
    with ScalaCheckPropertyChecks
    with ScalaFutures
    with Config
    with BeforeAndAfterAll
    with StrictLogging {

  implicit val ec: ExecutionContext            = system.dispatcher
  implicit val defaultPatience: PatienceConfig = PatienceConfig(90 seconds, 100 millis)

  val ThrottleElements: Int          = 100
  val ThrottleAmount: FiniteDuration = 1 millis

  val s3Settings: S3Settings

  implicit lazy val s3Attrs: Attributes = S3Attributes.settings(s3Settings)

  /** Whether to use virtual dot host, Typically this is disabled when testing against real services because they
    * require domain name verification
    */
  val useVirtualDotHost: Boolean

  /** A prefix that will get added to each generated bucket in the test, this is to track the buckets that are
    * specifically created by the test
    */
  lazy val bucketPrefix: Option[String] = None

  private val bucketsToCleanup = new ConcurrentLinkedQueue[String]()

  def createBucket(bucket: String): Future[Unit] =
    for {
      _ <- S3.makeBucket(bucket)
      _ = if (enableCleanup.isDefined)
            bucketsToCleanup.add(bucket)
    } yield ()

  /** Whether to enable cleanup of buckets after tests are run and if so the initial delay to wait after the test
    */
  lazy val enableCleanup: Option[FiniteDuration] = None

  /** The MaxTimeout when cleaning up all of the buckets during `afterAll`
    */
  lazy val maxCleanupTimeout: FiniteDuration = 10 minutes

  private def cleanBucket(bucket: String): Future[Unit] = (for {
    check <- S3.checkIfBucketExists(bucket)
    _ <- check match {
           case BucketAccess.AccessDenied =>
             Future {
               logger.warn(
                 s"Cannot delete bucket: $bucket due to having access denied. Please look into this as it can fill up your AWS account"
               )
             }
           case BucketAccess.AccessGranted =>
             logger.info(s"Cleaning up bucket: $bucket")
             for {
               _          <- S3.deleteBucketContents(bucket).runWith(Sink.ignore)
               multiParts <- S3.listMultipartUpload(bucket, None).runWith(Sink.seq)
               _ <- Future.sequence(multiParts.map { part =>
                      for {
                        _ <- S3.deleteUpload(bucket, part.key, part.uploadId)
                      } yield ()
                    })
               _ <- S3.deleteBucket(bucket)
             } yield ()
           case BucketAccess.NotExists =>
             Future {
               logger.info(s"Not deleting bucket: $bucket since it no longer exists")
             }
         }

  } yield ()).recover { case util.control.NonFatal(error) =>
    logger.error(s"Error deleting bucket: $bucket", error)
  }

  override def afterAll(): Unit =
    enableCleanup match {
      case Some(initialDelay) =>
        def cleanAllBuckets = {
          val futures = bucketsToCleanup.asScala.toList.distinct.map(cleanBucket)
          Future.sequence(futures)
        }

        Await.result(akka.pattern.after(initialDelay)(cleanAllBuckets), maxCleanupTimeout)
      case None => ()
    }

  property("backup method completes flow correctly for all valid Kafka events") {
    forAll(kafkaDataWithTimePeriodsGen(), s3ConfigGen(useVirtualDotHost, bucketPrefix)) {
      (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod, s3Config: S3Config) =>
        logger.info(s"Data bucket is ${s3Config.dataBucket}")
        val backupClient = new MockedS3BackupClientInterface(kafkaDataWithTimePeriod.data,
                                                             kafkaDataWithTimePeriod.periodSlice,
                                                             s3Config,
                                                             Some(s3Settings),
                                                             Some(_.throttle(ThrottleElements, ThrottleAmount))
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
        val observed = calculatedFuture.futureValue

        kafkaDataWithTimePeriod.data.containsSlice(observed) mustEqual true
        if (observed.nonEmpty) {
          observed.head must matchTo(kafkaDataWithTimePeriod.data.head)
        }
    }
  }
}
