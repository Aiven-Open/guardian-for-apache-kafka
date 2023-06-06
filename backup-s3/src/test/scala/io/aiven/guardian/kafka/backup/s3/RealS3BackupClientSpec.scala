package io.aiven.guardian.kafka.backup.s3

import io.aiven.guardian.kafka.backup.configs.Compression
import io.aiven.guardian.pekko.AnyPropTestKit
import org.apache.pekko.actor.ActorSystem

class RealS3BackupClientSpec extends AnyPropTestKit(ActorSystem("RealS3BackupClientSpec")) with RealS3BackupClientTest {
  override val compression: Option[Compression] = None
}
