package io.aiven.guardian.kafka.restore

import io.aiven.guardian.kafka.backup.configs.Compression
import io.aiven.guardian.kafka.models.Gzip
import io.aiven.guardian.pekko.AnyPropTestKit
import org.apache.pekko.actor.ActorSystem

class GzipCompressionRestoreClientInterfaceSpec
    extends AnyPropTestKit(ActorSystem("GzipCompressionRestoreClientInterfaceSpec"))
    with RestoreClientInterfaceTest {
  override val compression: Option[Compression] = Some(Compression(Gzip, None))
}
