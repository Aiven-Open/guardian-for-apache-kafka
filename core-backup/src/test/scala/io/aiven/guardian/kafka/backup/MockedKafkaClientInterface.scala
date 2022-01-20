package io.aiven.guardian.kafka.backup

import akka.Done
import akka.NotUsed
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.SourceWithContext
import io.aiven.guardian.kafka.models.ReducedConsumerRecord

import scala.collection.immutable
import scala.concurrent.Future

import java.util.concurrent.ConcurrentLinkedQueue

/** A mocked `KafkaClientInterface` that returns a specific data as its source
  * @param kafkaData
  *   The data which the mock will output
  * @param sourceTransform
  *   A function that allows you to transform the source in some way. Convenient for cases such as throttling. By
  *   default this is `None` so it just preserves the original source.
  */
class MockedKafkaClientInterface(kafkaData: Source[ReducedConsumerRecord, NotUsed]) extends KafkaClientInterface {

  /** A collection that keeps track of whenever a cursor is committed
    */
  val committedOffsets: ConcurrentLinkedQueue[Long] = new ConcurrentLinkedQueue[Long]()

  /** The type of the context to pass around. In context of a Kafka consumer, this typically holds offset data to be
    * automatically committed
    */
  override type CursorContext = Long

  /** The type that represents how to control the given stream, i.e. if you want to shut it down or add metrics
    */
  override type Control = Future[NotUsed]

  /** The type that represents the result of batching a `CursorContext`
    */
  override type BatchedCursorContext = Long

  /** @return
    *   A `SourceWithContext` that returns a Kafka Stream which automatically handles committing of cursors
    */
  override def getSource: SourceWithContext[ReducedConsumerRecord, Long, Future[NotUsed]] =
    SourceWithContext
      .fromTuples(kafkaData.map { reducedConsumerRecord =>
        (reducedConsumerRecord, reducedConsumerRecord.offset)
      })
      .mapMaterializedValue(Future.successful)

  /** @return
    *   A `Sink` that allows you to commit a `CursorContext` to Kafka to signify you have processed a message
    */
  override def commitCursor: Sink[Long, Future[Done]] = Sink.foreach(cursor => committedOffsets.add(cursor))

  /** How to batch an immutable iterable of `CursorContext` into a `BatchedCursorContext`
    * @param cursors
    *   The cursors that need to be batched
    * @return
    *   A collection data structure that represents the batched cursors
    */
  override def batchCursorContext(cursors: immutable.Iterable[Long]): Long = cursors.max

}
