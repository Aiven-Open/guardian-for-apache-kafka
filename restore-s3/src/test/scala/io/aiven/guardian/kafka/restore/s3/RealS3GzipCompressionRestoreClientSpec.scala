package io.aiven.guardian.kafka.restore.s3

import io.aiven.guardian.kafka.backup.configs.Compression
import io.aiven.guardian.kafka.models.Gzip
import io.aiven.guardian.pekko.AnyPropTestKit
import org.apache.pekko.actor.ActorSystem

class RealS3GzipCompressionRestoreClientSpec
    extends AnyPropTestKit(ActorSystem("RealS3GzipCompressionRestoreClientSpec"))
    with RealS3RestoreClientTest {
  override val compression: Option[Compression] = Some(Compression(Gzip, None))
}
