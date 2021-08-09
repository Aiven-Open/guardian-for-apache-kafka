package io.aiven.guardian.kafka

import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import akka.stream.scaladsl.{Sink, Source, SourceWithContext}
import akka.{Done, NotUsed}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

/** A mocked `KafkaClientInterface` that returns a specific data as its source
  * @param kafkaData The data which the mock will output
  */
class MockedKafkaClientInterface(kafkaData: List[ReducedConsumerRecord]) extends KafkaClientInterface {

  /** A collection that keeps track of whenever a cursor is committed
    */
  val committedOffsets = new ConcurrentLinkedQueue[Long]().asScala

  /** The type of the context to pass around. In context of a Kafka consumer, this typically holds offset data to be
    * automatically committed
    */
  override type CursorContext = Long

  /** The type that represents how to control the given stream, i.e. if you want to shut it down or add metrics
    */
  override type Control = NotUsed

  /** @return A `SourceWithContext` that returns a Kafka Stream which automatically handles committing of cursors
    */
  override def getSource: SourceWithContext[ReducedConsumerRecord, Long, NotUsed] =
    SourceWithContext.fromTuples(Source(kafkaData.map { reducedConsumerRecord =>
      (reducedConsumerRecord, reducedConsumerRecord.offset)
    }))

  /** @return A `Sink` that allows you to commit a `CursorContext` to Kafka to signify you have processed a message
    */
  override def commitCursor: Sink[Long, Future[Done]] = Sink.foreach(cursor => committedOffsets ++ Iterable(cursor))

}
