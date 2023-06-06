package io.aiven.guardian.kafka.s3.configs

import org.apache.pekko.stream.RestartSettings

/** S3 specific configuration used when storing Kafka ConsumerRecords to a S3 bucket
  *
  * @param dataBucket
  *   The bucket where a Kafka Consumer directly streams data into as storage
  * @param dataBucketPrefix
  *   Prefix for the data bucket (if any)
  * @param errorRestartSettings
  *   Restart settings that are used whenever an pekko-stream encounters an error
  */
final case class S3(dataBucket: String, dataBucketPrefix: Option[String], errorRestartSettings: RestartSettings)

object S3 {
  def apply(dataBucket: String, errorRestartSettings: RestartSettings): S3 = S3(dataBucket, None, errorRestartSettings)
}
