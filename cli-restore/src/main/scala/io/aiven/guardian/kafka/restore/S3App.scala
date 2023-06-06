package io.aiven.guardian.kafka.restore

import com.typesafe.scalalogging.LazyLogging
import io.aiven.guardian.kafka.restore.s3.RestoreClient
import io.aiven.guardian.kafka.s3.{Config => S3Config}
import org.apache.pekko

import pekko.stream.ActorAttributes
import pekko.stream.Attributes
import pekko.stream.Supervision
import pekko.stream.connectors.s3.S3Settings

trait S3App extends S3Config with RestoreApp with App with LazyLogging {
  lazy val s3Settings: S3Settings = S3Settings()
  implicit lazy val restoreClient: RestoreClient[KafkaProducer] =
    new RestoreClient[KafkaProducer](Some(s3Settings)) {
      override val maybeAttributes: Some[Attributes] = {
        val decider: Supervision.Decider = { e =>
          logger.error("Unhandled exception in stream", e)
          Supervision.Stop
        }

        Some(ActorAttributes.supervisionStrategy(decider))
      }
    }
}
