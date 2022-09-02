package io.aiven.guardian.kafka.backup.s3

import akka.actor.ActorSystem
import io.aiven.guardian.akka.AnyPropTestKit
import io.aiven.guardian.kafka.backup.configs.Compression
import io.aiven.guardian.kafka.models.Gzip

class RealS3GzipCompressionBackupClientSpec
    extends AnyPropTestKit(ActorSystem("RealS3GzipCompressionBackupClientSpec"))
    with RealS3BackupClientTest {
  override val compression: Option[Compression] = Some(Compression(Gzip, None))
}
