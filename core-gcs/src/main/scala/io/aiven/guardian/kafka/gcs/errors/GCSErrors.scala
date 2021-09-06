package io.aiven.guardian.kafka.gcs.errors

import io.aiven.guardian.kafka.Errors

sealed abstract class GCSErrors extends Errors

object GCSErrors {
  final case class ExpectedObjectToExist(bucketName: String, key: String, prefix: Option[String]) extends GCSErrors {
    override def getMessage: String =
      s"GCS object key:$key inside bucket:$bucketName doesn't exist"
  }

}
