package io.aiven.guardian.kafka.s3

import com.typesafe.scalalogging.StrictLogging
import markatta.futiles.Retry
import org.apache.pekko

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import pekko.actor.ActorSystem
import pekko.stream.Attributes
import pekko.stream.connectors.s3.scaladsl.S3
import pekko.stream.scaladsl.Sink

object S3TestUtils extends StrictLogging {

  /** Completely cleans a bucket contents as well as deleting it afterwards.
    */
  def cleanAndDeleteBucket(bucket: String)(implicit system: ActorSystem, s3Attrs: Attributes): Future[Unit] = {
    implicit val ec: ExecutionContext = system.dispatcher
    for {
      _ <- S3.deleteBucketContents(bucket, deleteAllVersions = true).withAttributes(s3Attrs).runWith(Sink.ignore)
      multiParts <-
        S3.listMultipartUpload(bucket, None).withAttributes(s3Attrs).runWith(Sink.seq)
      _ <- Future.sequence(multiParts.map { part =>
             S3.deleteUpload(bucket, part.key, part.uploadId)
           })
      _ <- Retry.retryWithBackOff(
             5,
             100 millis,
             throwable => throwable.getMessage.contains("The bucket you tried to delete is not empty")
           )(S3.deleteBucket(bucket))
      _ = logger.info(s"Completed deleting bucket $bucket")
    } yield ()
  }

}
