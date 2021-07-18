package aiven.io.guardian.kafka.s3.errors

import akka.http.scaladsl.model.headers.ByteRange
import akka.stream.alpakka.s3.S3Headers

sealed abstract class S3Errors extends Exception

object S3Errors {
  final case class ExpectedObjectToExist(bucket: String,
                                         key: String,
                                         range: Option[ByteRange],
                                         versionId: Option[String],
                                         s3Headers: S3Headers
  ) extends S3Errors {
    override def getMessage: String = {
      val finalVersionId = versionId.getOrElse("latest")
      s"S3 object key:$key and version:$finalVersionId inside bucket:$bucket doesn't exist. S3 headers are ${s3Headers.toString()}"
    }
  }
}
