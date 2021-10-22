package io.aiven.guardian.kafka

import akka.testkit.TestKitBase
import com.dimafeng.testcontainers.ForAllTestContainer
import com.dimafeng.testcontainers.KafkaContainer
import org.scalatest.Suite

trait KafkaClusterTest extends ForAllTestContainer with TestKitBase { this: Suite =>
  override lazy val container: KafkaContainer = new KafkaContainer()
}
