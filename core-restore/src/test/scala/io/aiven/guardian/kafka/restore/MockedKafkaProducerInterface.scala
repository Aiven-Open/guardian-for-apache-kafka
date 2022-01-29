package io.aiven.guardian.kafka.restore
import akka.Done
import akka.stream.scaladsl.Sink
import io.aiven.guardian.kafka.models.ReducedConsumerRecord

import scala.concurrent.Future

import java.util.concurrent.ConcurrentLinkedQueue

class MockedKafkaProducerInterface() extends KafkaProducerInterface {
  val producedData: ConcurrentLinkedQueue[ReducedConsumerRecord] = new ConcurrentLinkedQueue[ReducedConsumerRecord]()

  override def getSink: Sink[ReducedConsumerRecord, Future[Done]] =
    Sink.foreach[ReducedConsumerRecord] { reducedConsumerRecord =>
      producedData.add(reducedConsumerRecord)
    }
}
