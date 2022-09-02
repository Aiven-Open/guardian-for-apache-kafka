package io.aiven.guardian.kafka.restore

import akka.actor.ActorSystem
import io.aiven.guardian.akka.AnyPropTestKit
import io.aiven.guardian.kafka.backup.configs.Compression
import io.aiven.guardian.kafka.models.Gzip

class GzipCompressionRestoreClientInterfaceSpec
    extends AnyPropTestKit(ActorSystem("GzipCompressionRestoreClientInterfaceSpec"))
    with RestoreClientInterfaceTest {
  override val compression: Option[Compression] = Some(Compression(Gzip, None))
}
