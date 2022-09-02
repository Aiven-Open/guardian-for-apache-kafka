package io.aiven.guardian.kafka.backup

import akka.actor.ActorSystem
import io.aiven.guardian.akka.AnyPropTestKit
import io.aiven.guardian.kafka.backup.configs.Compression

class BackupClientInterfaceSpec
    extends AnyPropTestKit(ActorSystem("BackupClientInterfaceSpec"))
    with BackupClientInterfaceTest {
  override val compression: Option[Compression] = None
}
