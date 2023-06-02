package io.aiven.guardian.kafka.backup

import io.aiven.guardian.kafka.backup.configs.Compression
import io.aiven.guardian.pekko.AnyPropTestKit
import org.apache.pekko.actor.ActorSystem

class BackupClientInterfaceSpec
    extends AnyPropTestKit(ActorSystem("BackupClientInterfaceSpec"))
    with BackupClientInterfaceTest {
  override val compression: Option[Compression] = None
}
