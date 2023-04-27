package io.aiven.guardian.kafka.s3

import akka.actor.ActorSystem
import akka.stream.Attributes
import akka.stream.alpakka.s3.BucketAccess
import akka.stream.alpakka.s3.scaladsl.S3
import io.aiven.guardian.kafka.s3.configs.{S3 => S3Config}
import org.scalacheck.Gen

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object UniqueGenerators {

  def uniqueS3ConfigGen(useVirtualDotHost: Boolean, prefix: Option[String] = None)(implicit
      system: ActorSystem,
      s3Attrs: Attributes
  ): Gen[S3Config] =
    for {
      s3Config <- Generators.s3ConfigGen(useVirtualDotHost, prefix)
      check = Await.result(S3.checkIfBucketExists(s3Config.dataBucket), Duration.Inf)
      bucket <- check match {
                  case BucketAccess.AccessDenied  => uniqueS3ConfigGen(useVirtualDotHost, prefix)
                  case BucketAccess.AccessGranted => uniqueS3ConfigGen(useVirtualDotHost, prefix)
                  case BucketAccess.NotExists     => Gen.const(s3Config)
                }
    } yield bucket

}
