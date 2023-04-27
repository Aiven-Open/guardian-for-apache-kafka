package io.aiven.guardian.kafka

import org.scalacheck.Gen
import org.scalatest.Suite

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

trait UniqueGenerators extends KafkaClusterTest { this: Suite =>
  private def uniqueKafkaConsumerGroupGenRec()(implicit executionContext: ExecutionContext): Gen[String] =
    for {
      kafkaConsumerGroup <- Generators.kafkaConsumerGroupGen
      exists = Await.result(checkConsumerGroupExists(kafkaConsumerGroup), Duration.Inf)
      result <- if (exists)
                  uniqueKafkaConsumerGroupGenRec()
                else
                  Gen.const(kafkaConsumerGroup)
    } yield result

  def uniqueKafkaConsumerGroupGen()(implicit executionContext: ExecutionContext): Gen[String] = {
    // Its possible for a container to not be started so just referenced the variable causes the lazy value
    // to initialize.
    container
    uniqueKafkaConsumerGroupGenRec()
  }

}
