package io.aiven.guardian.kafka.restore

import akka.stream.alpakka.s3.S3Settings
import io.aiven.guardian.kafka.restore.s3.RestoreClient
import io.aiven.guardian.kafka.s3.{Config => S3Config}

trait S3App extends S3Config with RestoreApp with App {
  lazy val s3Settings: S3Settings = S3Settings()
  implicit lazy val restoreClient: RestoreClient[KafkaProducer] =
    new RestoreClient[KafkaProducer](Some(s3Settings), maybeKillSwitch)
}
