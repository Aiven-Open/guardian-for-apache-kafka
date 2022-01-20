package io.aiven.guardian.kafka.s3.configs

/** S3 specific configuration used when storing Kafka ConsumerRecords to a S3 bucket
  * @param dataBucket
  *   The bucket where a Kafka Consumer directly streams data into as storage
  * @param dataBucketPrefix
  *   Prefix for the data bucket (if any)
  */
final case class S3(dataBucket: String, dataBucketPrefix: Option[String])

object S3 {
  def apply(dataBucket: String): S3 = S3(dataBucket, None)
}
