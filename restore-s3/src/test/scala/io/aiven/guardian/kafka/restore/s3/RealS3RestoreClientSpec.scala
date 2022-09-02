package io.aiven.guardian.kafka.restore.s3

import akka.actor.ActorSystem
import io.aiven.guardian.akka.AnyPropTestKit
import io.aiven.guardian.kafka.backup.configs.Compression

class RealS3RestoreClientSpec
    extends AnyPropTestKit(ActorSystem("RealS3RestoreClientSpec"))
    with RealS3RestoreClientTest {
  override val compression: Option[Compression] = None
}
