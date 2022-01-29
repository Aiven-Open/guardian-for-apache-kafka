package io.aiven.guardian.kafka.restore

import akka.Done
import akka.stream.scaladsl.Sink
import io.aiven.guardian.kafka.models.ReducedConsumerRecord

import scala.concurrent.Future

trait KafkaProducerInterface {
  def getSink: Sink[ReducedConsumerRecord, Future[Done]]
}
