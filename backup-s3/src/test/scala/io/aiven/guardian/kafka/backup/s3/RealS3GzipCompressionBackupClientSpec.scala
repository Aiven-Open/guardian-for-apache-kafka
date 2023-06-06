package io.aiven.guardian.kafka.backup.s3

import io.aiven.guardian.kafka.backup.configs.Compression
import io.aiven.guardian.kafka.models.Gzip
import io.aiven.guardian.pekko.AnyPropTestKit
import org.apache.pekko.actor.ActorSystem

class RealS3GzipCompressionBackupClientSpec
    extends AnyPropTestKit(ActorSystem("RealS3GzipCompressionBackupClientSpec"))
    with RealS3BackupClientTest {
  override val compression: Option[Compression] = Some(Compression(Gzip, None))
}
