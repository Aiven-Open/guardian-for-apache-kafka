package io.aiven.guardian.kafka

import scala.concurrent.Future

import akka.Done
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.SourceWithContext
import io.aiven.guardian.kafka.models.ReducedConsumerRecord

trait KafkaClientInterface {

  /** The type of the context to pass around. In context of a Kafka consumer, this typically holds offset data to be
    * automatically committed
    */
  type CursorContext

  /** The type that represents how to control the given stream, i.e. if you want to shut it down or add metrics
    */
  type Control

  /** @return
    *   A `SourceWithContext` that returns a Kafka Stream which automatically handles committing of cursors
    */
  def getSource: SourceWithContext[ReducedConsumerRecord, CursorContext, Control]

  /** @return
    *   A `Sink` that allows you to commit a `CursorContext` to Kafka to signify you have processed a message
    */
  def commitCursor: Sink[CursorContext, Future[Done]]
}
