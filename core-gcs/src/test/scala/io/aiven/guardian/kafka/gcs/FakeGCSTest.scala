package io.aiven.guardian.kafka.gcs

import akka.stream.alpakka.googlecloud.storage.GCSSettings
import akka.testkit.TestKitBase
import com.dimafeng.testcontainers.ForAllTestContainer
import org.scalatest.Suite

trait FakeGCSTest extends ForAllTestContainer with TestKitBase { this: Suite =>
  override val container: FakeGCSContainer = new FakeGCSContainer()

  lazy val gcsSettings: GCSSettings = GCSSettings().withEndpointUrl(s"http://${container.getHostAddress}")
}
