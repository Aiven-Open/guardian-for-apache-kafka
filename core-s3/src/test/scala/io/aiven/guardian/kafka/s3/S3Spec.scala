package io.aiven.guardian.kafka.s3

import akka.NotUsed
import akka.actor.Scheduler
import akka.stream.Attributes
import akka.stream.alpakka.s3.BucketAccess
import akka.stream.alpakka.s3.ListBucketResultContents
import akka.stream.alpakka.s3.S3Attributes
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.testkit.TestKitBase
import com.typesafe.scalalogging.LazyLogging
import io.aiven.guardian.akka.AkkaHttpTestKit
import io.aiven.guardian.kafka.TestUtils
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.Ignore
import org.scalatest.Tag
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.propspec.AnyPropSpecLike
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

import java.util.Base64
import java.util.concurrent.ConcurrentLinkedQueue

trait S3Spec
    extends TestKitBase
    with AnyPropSpecLike
    with AkkaHttpTestKit
    with ScalaCheckPropertyChecks
    with ScalaFutures
    with Config
    with LazyLogging {

  // See https://stackoverflow.com/a/38834773
  case object RealS3Available
      extends Tag(
        if (TestUtils.checkEnvVarAvailable("ALPAKKA_S3_AWS_CREDENTIALS_ACCESS_KEY_ID")) "" else classOf[Ignore].getName
      )

  implicit val ec: ExecutionContext            = system.dispatcher
  implicit val defaultPatience: PatienceConfig = PatienceConfig(10 minutes, 100 millis)
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1, minSize = 1)

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

  /** Whether to enable cleanup of buckets after tests are run and if so the initial delay to wait after the test
    */
  lazy val enableCleanup: Option[FiniteDuration] = None

  /** The MaxTimeout when cleaning up all of the buckets during `afterAll`
    */
  lazy val maxCleanupTimeout: FiniteDuration = 10 minutes

  def createBucket(bucket: String): Future[Unit] =
    for {
      bucketResponse <- S3.checkIfBucketExists(bucket)
      _ <- bucketResponse match {
             case BucketAccess.AccessDenied =>
               throw new RuntimeException(
                 s"Unable to create bucket: $bucket since it already exists however permissions are inadequate"
               )
             case BucketAccess.AccessGranted =>
               logger.info(s"Deleting and recreating bucket: $bucket since it already exists with correct permissions")
               for {
                 _ <- S3.deleteBucketContents(bucket).withAttributes(s3Attrs).runWith(Sink.ignore)
                 // Although theoretically speaking just clearing the buckets should be enough, since we rely a lot
                 // on multipart upload state its safer to just delete and recreate the bucket
                 _ <- S3.deleteBucket(bucket)
                 _ <- S3.makeBucket(bucket)
               } yield ()
             case BucketAccess.NotExists =>
               S3.makeBucket(bucket)
           }
      _ = if (enableCleanup.isDefined)
            bucketsToCleanup.add(bucket)
    } yield ()

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
               _          <- S3.deleteBucketContents(bucket, deleteAllVersions = true).runWith(Sink.ignore)
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

  /** @param dataBucket
    *   Which S3 bucket the objects are being persisted into
    * @param transformResult
    *   A function that transforms the download result from S3 into the data `T` that you need. Note that you can also
    *   throw an exception in this transform function to trigger a retry (i.e. using it as a an additional predicate)
    * @param attempts
    *   Total number of attempts
    * @param delay
    *   The delay between each attempt after the first
    * @tparam T
    *   Type of the final result transformed by `transformResult`
    * @return
    */
  def waitForS3Download[T](dataBucket: String,
                           transformResult: Seq[ListBucketResultContents] => T,
                           attempts: Int = 10,
                           delay: FiniteDuration = 1 second
  ): Future[T] = {
    implicit val scheduler: Scheduler = system.scheduler

    val attempt = () =>
      S3.listBucket(dataBucket, None).withAttributes(s3Attrs).runWith(Sink.seq).map {
        transformResult
      }

    akka.pattern.retry(attempt, attempts, delay)
  }

  case class DownloadNotReady(downloads: Seq[ListBucketResultContents])
      extends Exception(s"Download not ready, current state is ${downloads.map(_.toString).mkString(",")}")

  def reducedConsumerRecordsToJson(reducedConsumerRecords: List[ReducedConsumerRecord]): Array[Byte] = {
    import io.aiven.guardian.kafka.codecs.Circe._
    import io.circe.syntax._
    reducedConsumerRecords.asJson.noSpaces.getBytes
  }

  /** Converts a list of `ProducerRecord` to a source that is streamed over a period of time
    * @param producerRecords
    *   The list of producer records
    * @param streamDuration
    *   The period over which the topics will be streamed
    * @return
    *   Source ready to be passed into a Kafka producer
    */
  def toSource(
      producerRecords: List[ProducerRecord[Array[Byte], Array[Byte]]],
      streamDuration: FiniteDuration
  ): Source[ProducerRecord[Array[Byte], Array[Byte]], NotUsed] = {
    val durationToMicros = streamDuration.toMillis
    val topicsPerMillis  = producerRecords.size / durationToMicros
    Source(producerRecords).throttle(topicsPerMillis.toInt, 1 millis)
  }

  /** Converts a generated list of `ReducedConsumerRecord` to a list of `ProducerRecord`
    * @param data
    *   List of `ReducedConsumerRecord`'s generated by scalacheck
    * @return
    *   A list of `ProducerRecord`. Note that it only uses the `key`/`value` and ignores other values
    */
  def toProducerRecords(data: List[ReducedConsumerRecord]): List[ProducerRecord[Array[Byte], Array[Byte]]] = data.map {
    reducedConsumerRecord =>
      val valueAsByteArray = Base64.getDecoder.decode(reducedConsumerRecord.value)
      reducedConsumerRecord.key match {
        case Some(key) =>
          new ProducerRecord[Array[Byte], Array[Byte]](reducedConsumerRecord.topic,
                                                       Base64.getDecoder.decode(key),
                                                       valueAsByteArray
          )
        case None =>
          new ProducerRecord[Array[Byte], Array[Byte]](reducedConsumerRecord.topic, valueAsByteArray)
      }
  }
}
