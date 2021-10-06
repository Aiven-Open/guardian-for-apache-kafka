package io.aiven.guardian.kafka.backup.s3

import akka.actor.ActorSystem
import io.aiven.guardian.akka.AnyPropTestKit
import io.aiven.guardian.kafka.s3.MinioS3Test

class MinioBackupClientSpec
    extends AnyPropTestKit(ActorSystem("MinioS3BackupClientSpec"))
    with BackupClientSpec
    with MinioS3Test {

  /** Since Minio doesn't do DNS name verification we can enable this
    */
  override lazy val useVirtualDotHost: Boolean = true
}
