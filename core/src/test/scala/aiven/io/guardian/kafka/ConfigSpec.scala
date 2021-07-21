package aiven.io.guardian.kafka

import aiven.io.guardian.kafka.configs.KafkaCluster
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import pureconfig.ConfigSource

class ConfigSpec extends AnyWordSpec with Matchers with ScalaCheckPropertyChecks {
  implicit val kafkaClusterArb = Arbitrary(
    Gen.containerOf[Set, String](Gen.alphaStr).map(topics => KafkaCluster(topics))
  )

  "Load KafkaClusterConfig with valid topics" in {
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
