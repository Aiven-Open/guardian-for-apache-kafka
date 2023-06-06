package io.aiven.guardian.kafka.restore

import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import org.apache.pekko

import scala.concurrent.Future

import pekko.Done
import pekko.stream.scaladsl.Sink

trait KafkaProducerInterface {
  def getSink: Sink[ReducedConsumerRecord, Future[Done]]
}
