package io.aiven.guardian.kafka.backup

import akka.actor.ActorSystem
import io.aiven.guardian.akka.AnyPropTestKit
import io.aiven.guardian.kafka.backup.configs.Compression
import io.aiven.guardian.kafka.models.Gzip

class GzipCompressionBackupClientInterfaceSpec
    extends AnyPropTestKit(ActorSystem("GzipCompressionBackupClientInterfaceSpec"))
    with BackupClientInterfaceTest {
  override val compression: Option[Compression] = Some(Compression(Gzip, None))
}
