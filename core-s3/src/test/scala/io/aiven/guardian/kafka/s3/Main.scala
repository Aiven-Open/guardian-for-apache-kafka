package io.aiven.guardian.kafka.s3

import akka.actor.ActorSystem
import akka.stream.Attributes
import akka.stream.alpakka.s3.S3Attributes
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.Sink
import cats.data.NonEmptyList
import cats.implicits._
import com.monovore.decline.Command
import com.monovore.decline.CommandApp
import com.monovore.decline.Opts
import com.typesafe.scalalogging.LazyLogging
import io.aiven.guardian.kafka.s3.Entry.computeAndDeleteBuckets
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.AwsRegionProvider

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.control.NonFatal

class Entry
    extends CommandApp(
      name = "guardian-s3-test-utils",
      header = "Guardian S3 Test Utilities",
      main = {
        val cleanBucketsCommand = Command(
          name = "clean-buckets",
          header = "Clean buckets left over by Guardian S3 tests"
        ) {
          val prefixOpt: Opts[String] =
            Opts
              .option[String]("prefix", help = "Only delete buckets with specified prefix")

          val excludeBucketsOpt: Opts[Option[NonEmptyList[String]]] =
            Opts
              .options[String]("exclude-buckets",
                               help = "Buckets that will always be excluded from cleanup, irrespective of prefix"
              )
              .orNone

          (prefixOpt, excludeBucketsOpt).tupled
        }

        Opts.subcommand(cleanBucketsCommand).map { case (bucketPrefix, excludeBuckets) =>
          implicit val system: ActorSystem    = ActorSystem()
          implicit val ec: ExecutionContext   = system.dispatcher
          implicit val s3Settings: S3Settings = S3Settings()

          val excludeBucketsSet = excludeBuckets.map(_.toList.toSet).getOrElse(Set.empty)

          try {
            Await.result(computeAndDeleteBuckets(bucketPrefix, excludeBucketsSet), Duration.Inf)
            System.exit(0)
          } catch {
            case NonFatal(_) =>
              System.exit(1)
          }
        }
      }
    )

object Entry extends LazyLogging {
  def computeAndDeleteBuckets(bucketPrefix: String, excludeBuckets: Set[String])(implicit
      executionContext: ExecutionContext,
      system: ActorSystem,
      s3Settings: S3Settings
  ): Future[Set[String]] = for {
    bucketsToDelete <- computeBucketsToDelete(bucketPrefix, excludeBuckets)
    _ <- if (bucketsToDelete.nonEmpty) {
           deleteBuckets(bucketsToDelete)
         } else
           Future {
             logger.info("No buckets to delete")
           }
  } yield bucketsToDelete

  def computeBucketsToDelete(bucketPrefix: String, excludeBuckets: Set[String])(implicit
      system: ActorSystem,
      s3Settings: S3Settings
  ): Future[Set[String]] =
    // Bug that needs to be fixed upstream in Alpakka, this specific S3 api call is not region specific
    // so US_EAST_1 needs to be hardcoded
    S3.listBuckets()
      .withAttributes(S3Attributes.settings(s3Settings.withS3RegionProvider(new AwsRegionProvider {
        val getRegion: Region = Region.US_EAST_1
      })))
      .runWith(Sink.seq)
      .map { allBuckets =>
        allBuckets.map(_.name).toSet.filter(fromS3Bucket => fromS3Bucket.startsWith(bucketPrefix)).diff(excludeBuckets)
      }(ExecutionContext.parasitic)

  def deleteBuckets(
      buckets: Set[String]
  )(implicit executionContext: ExecutionContext, system: ActorSystem, s3Settings: S3Settings): Future[Unit] = {
    implicit val s3Attrs: Attributes = S3Attributes.settings(s3Settings)
    val futures = buckets.map { bucket =>
      logger.info(s"Deleting bucket $bucket")
      S3TestUtils.cleanAndDeleteBucket(bucket)
    }
    Future.sequence(futures).map(_ => ())(ExecutionContext.parasitic)
  }
}

object Main extends Entry
