package io.aiven.guardian.kafka.backup

import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import org.apache.pekko

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicReference

import pekko.Done
import pekko.NotUsed
import pekko.stream.scaladsl._

/** A mocked `KafkaClientInterface` that returns a specific data as its source
  *
  * @param kafkaData
  *   The data which the mock will output
  * @param commitStorage
  *   A collection that keeps track of whenever a cursor is committed
  * @param stopAfterDuration
  *   Dont produce any data from `kafkaData` if its offset is after `stopAfterOffset` based off of the first committed
  *   [[io.aiven.guardian.kafka.models.ReducedConsumerRecord.timestamp]]. Handy to simulate the premature closing of a
  *   KafkaClient before its completed producing all source elements (i.e. suspend/resume and restart scenarios).
  * @param handleOffsets
  *   Tells the MockedKafkaClientInterface to handleOffsets rather than just ignoring them. This means that the mock
  *   will only add commits to the `commitStorage` if its later than any currently processed offsets. Furthermore it
  *   will not replay source data if it has already been committed.
  */
class MockedKafkaConsumerInterface(kafkaData: Source[ReducedConsumerRecord, NotUsed],
                                   commitStorage: ConcurrentLinkedDeque[Long] = new ConcurrentLinkedDeque[Long](),
                                   stopAfterDuration: Option[FiniteDuration] = None,
                                   handleOffsets: Boolean = false
) extends KafkaConsumerInterface {

  /** The type of the context to pass around. In context of a Kafka consumer, this typically holds offset data to be
    * automatically committed
    */
  override type CursorContext = Long

  /** The type that represents how to control the given stream, i.e. if you want to shut it down or add metrics
    */
  override type Control = Future[NotUsed]

  /** The type that represents the result of the `combine` parameter that is supplied to
    * [[pekko.stream.scaladsl.Source.toMat]]
    */
  override type MatCombineResult = Future[NotUsed]

  /** The type that represents the result of batching a `CursorContext`
    */
  override type BatchedCursorContext = Long

  private val firstReducedConsumerRecord: AtomicReference[ReducedConsumerRecord] =
    new AtomicReference[ReducedConsumerRecord]()

  /** @return
    *   A `SourceWithContext` that returns a Kafka Stream which automatically handles committing of cursors
    */
  override def getSource: SourceWithContext[ReducedConsumerRecord, Long, Future[NotUsed]] = {
    val source = kafkaData
      .prefixAndTail(1)
      .flatMapConcat {
        case (Seq(head), rest) =>
          firstReducedConsumerRecord.set(head)
          Source.combine(
            Source.single(head),
            rest
          )(Concat(_))
        case _ => Source.empty[ReducedConsumerRecord]
      }

    val finalSource = if (handleOffsets) {
      source.filter { reducedConsumerRecord =>
        (commitStorage.isEmpty || {
          reducedConsumerRecord.offset > commitStorage.getLast
        }) && {
          (stopAfterDuration, Option(firstReducedConsumerRecord.get())) match {
            case (Some(afterDuration), Some(firstRecord)) =>
              val difference =
                ChronoUnit.MILLIS.between(Instant.ofEpochMilli(firstRecord.timestamp),
                                          Instant.ofEpochMilli(reducedConsumerRecord.timestamp)
                )
              afterDuration.toMillis > difference
            case _ => true
          }
        }
      }
    } else
      source

    SourceWithContext
      .fromTuples(finalSource.map { reducedConsumerRecord =>
        (reducedConsumerRecord, reducedConsumerRecord.offset)
      })
      .mapMaterializedValue(Future.successful)
  }

  /** @return
    *   The result of this function gets directly passed into the `combine` parameter of
    *   [[pekko.stream.scaladsl.Source.toMat]]
    */
  override def matCombine: (Future[NotUsed], Future[Done]) => Future[NotUsed] = Keep.left

  /** @return
    *   A `Sink` that allows you to commit a `CursorContext` to Kafka to signify you have processed a message
    */
  override def commitCursor: Sink[Long, Future[Done]] = Sink.foreach { cursor =>
    if (handleOffsets && !commitStorage.isEmpty) {
      if (commitStorage.getLast < cursor)
        commitStorage.add(cursor)
    } else
      commitStorage.add(cursor)
  }

  /** How to batch an immutable iterable of `CursorContext` into a `BatchedCursorContext`
    * @param cursors
    *   The cursors that need to be batched
    * @return
    *   A collection data structure that represents the batched cursors
    */
  override def batchCursorContext(cursors: immutable.Iterable[Long]): Long = cursors.max

}
