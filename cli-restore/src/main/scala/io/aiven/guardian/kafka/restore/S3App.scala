package io.aiven.guardian.kafka.restore

import akka.stream.{ActorAttributes, Attributes, Supervision}
import akka.stream.alpakka.s3.S3Settings
import com.typesafe.scalalogging.StrictLogging
import io.aiven.guardian.kafka.restore.s3.RestoreClient
import io.aiven.guardian.kafka.s3.{Config => S3Config}

trait S3App extends S3Config with RestoreApp with App with StrictLogging {
  lazy val s3Settings: S3Settings = S3Settings()
  implicit lazy val restoreClient: RestoreClient[KafkaProducer] =
    new RestoreClient[KafkaProducer](Some(s3Settings), maybeKillSwitch) {
      override val maybeAttributes: Some[Attributes] = {
        val decider: Supervision.Decider = { e =>
          logger.error("Unhandled exception in stream", e)
          Supervision.Stop
        }

        Some(ActorAttributes.supervisionStrategy(decider))
      }
    }
}
