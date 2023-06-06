package io.aiven.guardian.kafka.backup

import io.aiven.guardian.kafka.backup.configs.Compression
import io.aiven.guardian.kafka.models.Gzip
import io.aiven.guardian.pekko.AnyPropTestKit
import org.apache.pekko.actor.ActorSystem

class GzipCompressionBackupClientInterfaceSpec
    extends AnyPropTestKit(ActorSystem("GzipCompressionBackupClientInterfaceSpec"))
    with BackupClientInterfaceTest {
  override val compression: Option[Compression] = Some(Compression(Gzip, None))
}
