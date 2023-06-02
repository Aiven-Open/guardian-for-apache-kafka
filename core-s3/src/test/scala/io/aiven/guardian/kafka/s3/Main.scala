package io.aiven.guardian.kafka.s3

import cats.data.NonEmptyList
import cats.implicits._
import com.monovore.decline.Command
import com.monovore.decline.CommandApp
import com.monovore.decline.Opts
import com.typesafe.scalalogging.LazyLogging
import io.aiven.guardian.kafka.s3.Entry.computeAndDeleteBuckets
import org.apache.pekko

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.control.NonFatal

import pekko.actor.ActorSystem
import pekko.stream.Attributes
import pekko.stream.connectors.s3.S3Attributes
import pekko.stream.connectors.s3.S3Settings
import pekko.stream.connectors.s3.scaladsl.S3
import pekko.stream.scaladsl.Sink

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
    S3.listBuckets()
      .withAttributes(S3Attributes.settings(s3Settings))
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
