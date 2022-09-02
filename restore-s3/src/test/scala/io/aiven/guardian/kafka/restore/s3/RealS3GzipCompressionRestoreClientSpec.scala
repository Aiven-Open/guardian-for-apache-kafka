package io.aiven.guardian.kafka.restore.s3

import akka.actor.ActorSystem
import io.aiven.guardian.akka.AnyPropTestKit
import io.aiven.guardian.kafka.backup.configs.Compression
import io.aiven.guardian.kafka.models.Gzip

class RealS3GzipCompressionRestoreClientSpec
    extends AnyPropTestKit(ActorSystem("RealS3GzipCompressionRestoreClientSpec"))
    with RealS3RestoreClientTest {
  override val compression: Option[Compression] = Some(Compression(Gzip, None))
}
