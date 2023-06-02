package io.aiven.guardian.kafka.restore

import io.aiven.guardian.kafka.backup.configs.Compression
import io.aiven.guardian.pekko.AnyPropTestKit
import org.apache.pekko.actor.ActorSystem

class RestoreClientInterfaceSpec
    extends AnyPropTestKit(ActorSystem("RestoreClientInterfaceSpec"))
    with RestoreClientInterfaceTest {
  override val compression: Option[Compression] = None
}
