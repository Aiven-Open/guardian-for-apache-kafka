package io.aiven.guardian.kafka.backup.s3

import akka.actor.ActorSystem
import akka.stream.alpakka.s3.S3Settings
import io.aiven.guardian.akka.AnyPropTestKit

import scala.concurrent.duration._
import scala.language.postfixOps

class RealS3BackupClientSpec extends AnyPropTestKit(ActorSystem("RealS3BackupClientSpec")) with BackupClientSpec {
  override lazy val s3Settings: S3Settings = S3Settings()

  /** Virtual Dot Host in bucket names are disabled because you need an actual DNS certificate otherwise AWS will fail
    * on bucket creation
    */
  override lazy val useVirtualDotHost: Boolean            = false
  override lazy val bucketPrefix: Option[String]          = Some("guardian-")
  override lazy val enableCleanup: Option[FiniteDuration] = Some(5 seconds)
}
