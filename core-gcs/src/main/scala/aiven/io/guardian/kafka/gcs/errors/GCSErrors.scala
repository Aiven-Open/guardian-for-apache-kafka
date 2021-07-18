package aiven.io.guardian.kafka.gcs.errors

sealed abstract class GCSErrors extends Exception

object GCSErrors {
  final case class ExpectedObjectToExist(bucketName: String, maybePrefix: Option[String]) extends GCSErrors {
    override def getMessage: String =
      ???
  }

}
