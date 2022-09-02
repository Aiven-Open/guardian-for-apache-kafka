package io.aiven.guardian.kafka.restore

import akka.actor.ActorSystem
import io.aiven.guardian.akka.AnyPropTestKit
import io.aiven.guardian.kafka.backup.configs.Compression

class RestoreClientInterfaceSpec
    extends AnyPropTestKit(ActorSystem("RestoreClientInterfaceSpec"))
    with RestoreClientInterfaceTest {
  override val compression: Option[Compression] = None
}
