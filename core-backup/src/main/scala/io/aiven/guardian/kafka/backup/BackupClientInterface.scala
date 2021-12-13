package io.aiven.guardian.kafka.backup

import akka.NotUsed
import akka.stream.scaladsl._
import akka.util.ByteString
import io.aiven.guardian.kafka.Errors
import io.aiven.guardian.kafka.KafkaClientInterface
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.codecs.Circe._
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.circe.syntax._

import scala.annotation.nowarn
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal._

/** An interface for a template on how to backup a Kafka Stream into some data storage
  * @tparam T
  *   The underlying `kafkaClientInterface` type
  */
trait BackupClientInterface[T <: KafkaClientInterface] {
  implicit val kafkaClientInterface: T
  implicit val backupConfig: Backup

  /** An element from the original record
    */
  private[backup] sealed trait RecordElement
  private[backup] case class Element(reducedConsumerRecord: ReducedConsumerRecord,
                                     context: kafkaClientInterface.CursorContext
  ) extends RecordElement
  private[backup] case object End extends RecordElement

  /** An element after the record has been transformed to a ByteString
    */
  private[backup] sealed trait ByteStringElement {
    val data: ByteString
    val context: kafkaClientInterface.CursorContext
  }

  private[backup] case class Start(override val data: ByteString,
                                   override val context: kafkaClientInterface.CursorContext,
                                   key: String
  ) extends ByteStringElement
  private[backup] case class Tail(override val data: ByteString,
                                  override val context: kafkaClientInterface.CursorContext
  ) extends ByteStringElement

  /** Override this type to define the result of backing up data to a datasource
    */
  type BackupResult

  import BackupClientInterface._

  /** Override this method to define how to backup a `ByteString` to a `DataSource`
    * @param key
    *   The object key or filename for what is being backed up
    * @return
    *   A Sink that also provides a `BackupResult`
    */
  def backupToStorageSink(key: String): Sink[ByteString, Future[BackupResult]]

  /** Override this method to define a zero vale that covers the case that occurs immediately when `SubFlow` has been
    * split at `BackupStreamPosition.Start`. If you have difficulties defining an empty value for `BackupResult` then
    * you can wrap it in an `Option` and just use `None` for the empty case
    * @return
    *   An "empty" value that is used when a split `SubFlow` has just started
    */
  def empty: () => Future[BackupResult]

  private[backup] def calculateBackupStreamPositions(
      sourceWithPeriods: SourceWithContext[(ReducedConsumerRecord, Long),
                                           kafkaClientInterface.CursorContext,
                                           kafkaClientInterface.Control
      ]
  ): Source[RecordElement, kafkaClientInterface.Control] =
    sourceWithPeriods.asSource
      .prefixAndTail(2)
      // This algorithm only works with Source's that have 2 or more elements
      .flatMapConcat {
        case (Seq(
                ((firstReducedConsumerRecord, firstDivision), firstContext),
                ((secondReducedConsumerRecord, secondDivision), secondContext)
              ),
              rest
            ) =>
          val all = Source
            .combine(
              Source(
                List(
                  ((firstReducedConsumerRecord, firstDivision), firstContext),
                  ((secondReducedConsumerRecord, secondDivision), secondContext)
                )
              ),
              rest
            )(Concat(_))

          val withDivisions =
            all
              .sliding(2)
              .map {
                case Seq(((_, beforeDivisions), _), ((afterReducedConsumerRecord, afterDivisions), afterContext)) =>
                  if (isAtBoundary(beforeDivisions, afterDivisions))
                    List(
                      End,
                      Element(afterReducedConsumerRecord, afterContext)
                    )
                  else
                    List(Element(afterReducedConsumerRecord, afterContext))
                case rest =>
                  throw Errors.UnhandledStreamCase(rest)
              }
              .mapConcat(identity)

          Source.combine(
            Source.single(Element(firstReducedConsumerRecord, firstContext)),
            withDivisions
          )(Concat(_))
        // This case only occurs if a Source has a single element so we just directly return it
        case (Seq(((singleReducedConsumerRecord, _), singleContext)), _) =>
          Source.single(Element(singleReducedConsumerRecord, singleContext))
        case (rest, _) =>
          throw Errors.UnhandledStreamCase(rest)
      }

  private[backup] def sourceWithPeriods(
      source: Source[(OffsetDateTime, (ReducedConsumerRecord, kafkaClientInterface.CursorContext)),
                     kafkaClientInterface.Control
      ]
  ): SourceWithContext[(ReducedConsumerRecord, Long),
                       kafkaClientInterface.CursorContext,
                       kafkaClientInterface.Control
  ] = SourceWithContext.fromTuples(source.map { case (firstTimestamp, (record, context)) =>
    val period = calculateNumberOfPeriodsFromTimestamp(firstTimestamp, backupConfig.periodSlice, record)
    ((record, period), context)
  })

  private[backup] def sourceWithFirstRecord
      : Source[(OffsetDateTime, (ReducedConsumerRecord, kafkaClientInterface.CursorContext)),
               kafkaClientInterface.Control
      ] =
    kafkaClientInterface.getSource.asSource.prefixAndTail(1).flatMapConcat { case (head, rest) =>
      head.headOption match {
        case Some((firstReducedConsumerRecord, firstCursorContext)) =>
          val firstTimestamp = firstReducedConsumerRecord.toOffsetDateTime

          Source.combine(
            Source.single((firstTimestamp, (firstReducedConsumerRecord, firstCursorContext))),
            rest.map { case (reducedConsumerRecord, context) =>
              (firstTimestamp, (reducedConsumerRecord, context))
            }
          )(Concat(_))
        case None => throw Errors.ExpectedStartOfSource
      }
    }

  /** Transforms a sequence of [[RecordElement]]'s into a ByteString so that it can be persisted into the data storage
    *
    * @param sourceElements
    *   A sequence of [[RecordElement]]'s as a result of `sliding(2)`
    * @return
    *   a [[ByteString]] ready to be persisted along with the original context form the [[RecordElement]]
    */
  private[backup] def transformReducedConsumerRecords(sourceElements: Seq[RecordElement]) = {
    val stringWithContext = sourceElements match {
      case Seq(Element(reducedConsumerRecord, context)) =>
        // Happens in Sentinel case that is explicitly called at start of stream OR when a stream is interrupted by the user
        // in which case stream needs to be terminated with `null]` in order to be valid
        List((s"${reducedConsumerRecordAsString(reducedConsumerRecord)},", Some(context)))
      case Seq(Element(firstReducedConsumerRecord, firstContext),
               Element(secondReducedConsumerRecord, secondContext)
          ) =>
        List(
          (s"${reducedConsumerRecordAsString(firstReducedConsumerRecord)},", Some(firstContext)),
          (s"${reducedConsumerRecordAsString(secondReducedConsumerRecord)},", Some(secondContext))
        )
      case Seq(Element(reducedConsumerRecord, context), End) =>
        List((s"${reducedConsumerRecordAsString(reducedConsumerRecord)}]", Some(context)))
      case Seq(End) =>
        List(("]", None))
      case rest => throw Errors.UnhandledStreamCase(rest)
    }
    stringWithContext.map { case (string, context) => (ByteString(string), context) }
  }

  /** Applies the transformation to the first element of a Stream so that it starts of as a JSON array.
    *
    * @param element
    *   Starting [[Element]]
    * @param key
    *   The current key being processed
    * @param terminate
    *   Whether to immediately terminate the JSON array for single element in Stream case
    * @return
    *   A [[List]] containing a single [[Start]] ready to be processed in the [[Sink]]
    */
  private[backup] def transformFirstElement(element: Element, key: String, terminate: Boolean) =
    transformReducedConsumerRecords(List(element)).map {
      case (byteString, Some(context)) =>
        if (terminate)
          Start(ByteString("[") ++ byteString.dropRight(1) ++ ByteString("]"), context, key)
        else
          Start(ByteString("[") ++ byteString, context, key)
      case _ =>
        throw Errors.UnhandledStreamCase(List(element))
    }

  /** Fixes the case where is an odd amount of elements in the stream
    * @param head
    *   of stream as a result of `prefixAndTail`
    * @param restSource
    *   of the stream as a result of `prefixAndTail`
    * @return
    *   A [[List]] of ([[ByteString]], [[kafkaClientInterface.CursorContext]]) with the tail elements fixed up.
    */
  private[backup] def transformTailingElement(
      head: Seq[(ByteString, Option[kafkaClientInterface.CursorContext])],
      restSource: Source[(ByteString, Option[kafkaClientInterface.CursorContext]), NotUsed]
  ) = {
    val restTransformed = restSource
      .sliding(2, step = 2)
      .map {
        case Seq((before, Some(context)), (after, None)) =>
          List((before.dropRight(1) ++ after, context))
        case Seq((before, Some(beforeContext)), (after, Some(afterContext))) =>
          List((before, beforeContext), (after, afterContext))
        case Seq((single, Some(context))) =>
          List((single, context))
        case rest =>
          throw Errors.UnhandledStreamCase(rest)
      }

    head match {
      case Seq((byteString, Some(cursorContext))) =>
        Source.combine(
          Source.single((List((byteString, cursorContext)))),
          restTransformed
        )(Concat(_))
      case rest =>
        throw Errors.UnhandledStreamCase(rest)
    }
  }

  /** The entire flow that involves reading from Kafka, transforming the data into JSON and then backing it up into a
    * data source.
    * @return
    *   The `KafkaClientInterface.Control` which depends on the implementation of `T` (typically this is used to control
    *   the underlying stream).
    */
  def backup: RunnableGraph[kafkaClientInterface.Control] = {
    // TODO use https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3api/list-multipart-uploads.html
    // and https://stackoverflow.com/questions/53764876/resume-s3-multipart-upload-partetag to find any in progress
    // multiupload to resume from previous termination. Looks like we will have to do this manually since its not in
    // Alpakka yet

    val withBackupStreamPositions = calculateBackupStreamPositions(sourceWithPeriods(sourceWithFirstRecord))

    val split = withBackupStreamPositions
      .splitAfter { case sourceElement =>
        sourceElement match {
          case End => true
          case _   => false
        }
      }

    val substreams = split
      .prefixAndTail(2)
      .flatMapConcat {
        case (Seq(only: Element, End), _) =>
          // This case only occurs when you have a single element in a timeslice.
          // We have to terminate immediately to create a JSON array with a single element
          val key = calculateKey(only.reducedConsumerRecord.toOffsetDateTime)
          Source(transformFirstElement(only, key, terminate = true))
        case (Seq(first: Element, second: Element), restOfReducedConsumerRecords) =>
          val key         = calculateKey(first.reducedConsumerRecord.toOffsetDateTime)
          val firstSource = transformFirstElement(first, key, terminate = false)

          val rest = Source.combine(
            Source.single(second),
            restOfReducedConsumerRecords
          )(Concat(_))

          val restTransformed = rest
            .sliding(2, step = 2)
            .map(transformReducedConsumerRecords)
            .mapConcat(identity)
            .prefixAndTail(1)
            .flatMapConcat((transformTailingElement _).tupled)
            .mapConcat(identity)
            .map { case (byteString, context) => Tail(byteString, context) }

          Source.combine(
            Source(
              firstSource
            ),
            restTransformed
          )(Concat(_))
        case (Seq(only: Element), _) =>
          // This case can also occur when user terminates the stream
          val key = calculateKey(only.reducedConsumerRecord.toOffsetDateTime)
          Source(transformFirstElement(only, key, terminate = false))
        case (rest, _) =>
          throw Errors.UnhandledStreamCase(rest)
      }

    // Note that .alsoTo triggers after .to, see https://stackoverflow.com/questions/47895991/multiple-sinks-in-the-same-stream#comment93028098_47896071
    @nowarn("msg=method lazyInit in object Sink is deprecated")
    val subFlowSink = substreams
      .alsoTo(kafkaClientInterface.commitCursor.contramap[ByteStringElement] { byteStringElement =>
        byteStringElement.context
      })
      .to(
        // See https://stackoverflow.com/questions/68774425/combine-prefixandtail1-with-sink-lazysink-for-subflow-created-by-splitafter/68776660?noredirect=1#comment121558518_68776660
        Sink.lazyInit(
          {
            case start: Start =>
              Future.successful(
                backupToStorageSink(start.key).contramap[ByteStringElement] { byteStringElement =>
                  byteStringElement.data
                }
              )
            case _ => throw Errors.ExpectedStartOfSource
          },
          empty
        )
      )

    subFlowSink
  }
}

object BackupClientInterface {
  def reducedConsumerRecordAsString(reducedConsumerRecord: ReducedConsumerRecord): String =
    io.circe.Printer.noSpaces.print(reducedConsumerRecord.asJson)

  def formatOffsetDateTime(offsetDateTime: OffsetDateTime): String =
    offsetDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

  /** Calculate an object storage key or filename for a ReducedConsumerRecord
    * @param offsetDateTime
    *   A given time
    * @return
    *   A `String` that can be used either as some object key or a filename
    */
  def calculateKey(offsetDateTime: OffsetDateTime): String =
    s"${BackupClientInterface.formatOffsetDateTime(offsetDateTime)}.json"

  /** Calculates whether we have rolled over a time period given number of divided periods.
    * @param dividedPeriodsBefore
    *   The number of divided periods in the first element of the slide. -1 is used as a sentinel value to indicate the
    *   start of the stream
    * @param dividedPeriodsAfter
    *   The number of divided periods in the second element of the slide
    * @return
    *   `true` if we have hit a time boundary otherwise `false`
    */
  def isAtBoundary(dividedPeriodsBefore: Long, dividedPeriodsAfter: Long): Boolean =
    (dividedPeriodsBefore, dividedPeriodsAfter) match {
      case (before, after) if after > before =>
        true
      case _ =>
        false
    }

  protected def calculateNumberOfPeriodsFromTimestamp(initialTime: OffsetDateTime,
                                                      period: FiniteDuration,
                                                      reducedConsumerRecord: ReducedConsumerRecord
  ): Long =
    // TODO handle overflow?
    ChronoUnit.MICROS.between(initialTime, reducedConsumerRecord.toOffsetDateTime) / period.toMicros
}
