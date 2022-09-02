package io.aiven.guardian.kafka.backup.s3

import akka.actor.ActorSystem
import io.aiven.guardian.akka.AnyPropTestKit
import io.aiven.guardian.kafka.backup.configs.Compression

class RealS3BackupClientSpec extends AnyPropTestKit(ActorSystem("RealS3BackupClientSpec")) with RealS3BackupClientTest {
  override val compression: Option[Compression] = None
}
