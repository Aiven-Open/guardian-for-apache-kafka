package io.aiven.guardian.kafka.restore

import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import org.apache.pekko

import scala.concurrent.Future

import java.util.concurrent.ConcurrentLinkedQueue

import pekko.Done
import pekko.stream.scaladsl.Sink

class MockedKafkaProducerInterface() extends KafkaProducerInterface {
  val producedData: ConcurrentLinkedQueue[ReducedConsumerRecord] = new ConcurrentLinkedQueue[ReducedConsumerRecord]()

  override def getSink: Sink[ReducedConsumerRecord, Future[Done]] =
    Sink.foreach[ReducedConsumerRecord] { reducedConsumerRecord =>
      producedData.add(reducedConsumerRecord)
    }
}
