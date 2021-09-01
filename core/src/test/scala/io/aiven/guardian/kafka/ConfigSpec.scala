package io.aiven.guardian.kafka

import io.aiven.guardian.kafka.configs.KafkaCluster
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalatest.matchers.must.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import pureconfig.ConfigSource

class ConfigSpec extends AnyPropSpec with Matchers with ScalaCheckPropertyChecks {
  implicit val kafkaClusterArb = Arbitrary(
    Gen.containerOf[Set, String](Gen.alphaStr).map(topics => KafkaCluster(topics))
  )

  property("Valid KafkaClusterConfig configs should parse correctly") {
    forAll { (kafkaClusterConfig: KafkaCluster) =>
      val conf =
        s"""
        |kafka-cluster = {
        |  topics = [${kafkaClusterConfig.topics.map(s => "\"s\"").mkString(",")}]
        |}
        |""".stripMargin

      noException should be thrownBy ConfigSource.string(conf).at("kafka-cluster")
    }
  }
}
