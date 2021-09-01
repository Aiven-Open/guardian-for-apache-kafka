package io.aiven.guardian.kafka.backup

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal._

import scala.annotation.nowarn
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.stream.scaladsl._
import akka.util.ByteString
import io.aiven.guardian.kafka.Errors
import io.aiven.guardian.kafka.KafkaClientInterface
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.codecs.Circe._
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.circe.syntax._

/** A marker used to indicate in which position the current backup stream is
  */
sealed abstract class BackupStreamPosition

object BackupStreamPosition {

  /** The backup stream has just started right now
    */
  case object Start extends BackupStreamPosition

  /** The backup stream is in the middle of a time period
    */
  case object Middle extends BackupStreamPosition

  /** The backup stream position has just hit a boundary for when a new period starts
    */
  case object Boundary extends BackupStreamPosition
}

/** An interface for a template on how to backup a Kafka Stream into some data storage
  * @tparam T
  *   The underlying `kafkaClientInterface` type
  */
trait BackupClientInterface[T <: KafkaClientInterface] {
  implicit val kafkaClientInterface: T
  implicit val backupConfig: Backup

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

  @nowarn("msg=not.*?exhaustive")
  private[backup] def calculateBackupStreamPositions(
      sourceWithPeriods: SourceWithContext[(ReducedConsumerRecord, Long),
                                           kafkaClientInterface.CursorContext,
                                           kafkaClientInterface.Control
      ]
  ): SourceWithContext[(ReducedConsumerRecord, BackupStreamPosition),
                       kafkaClientInterface.CursorContext,
                       kafkaClientInterface.Control
  ] = sourceWithPeriods
    .sliding(2)
    .map { case Seq((beforeReducedConsumerRecord, beforeDivisions), (_, afterDivisions)) =>
      val backupStreamPosition = splitAtBoundaryCondition(beforeDivisions, afterDivisions)

      (beforeReducedConsumerRecord, backupStreamPosition)
    }
    .mapContext { case Seq(cursorContext, _) => cursorContext }

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
            rest.map { case (reducedConsumerRecord, context) => (firstTimestamp, (reducedConsumerRecord, context)) }
          )(Concat(_))
        case None => throw Errors.ExpectedStartOfSource
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

    val split = withBackupStreamPositions.asSource.splitAfter { case ((_, backupStreamPosition), _) =>
      backupStreamPosition == BackupStreamPosition.Boundary
    }

    val substreams = split
      .prefixAndTail(1)
      .flatMapConcat { case (head, restOfReducedConsumerRecords) =>
        head.headOption match {
          case Some(((firstReducedConsumerRecord, _), firstContext)) =>
            // We need to retrieve the first element of the stream in order to calculate the key/filename
            val key = calculateKey(firstReducedConsumerRecord.toOffsetDateTime)

            // Now that we have retrieved the first element of the stream, lets recombine it so we create the
            // original stream
            val combined = Source.combine(
              Source.single(
                (
                  (firstReducedConsumerRecord, BackupStreamPosition.Start),
                  firstContext
                )
              ),
              restOfReducedConsumerRecords
            )(Concat(_))

            // Go through every element in the stream and convert the `ReducedCustomerRecord` to an actual bytestream
            val transformed = combined.map { case ((record, position), context) =>
              val transform = transformReducedConsumerRecords(record, position)
              (transform, context)
            }

            transformed.map(data => (data, key))
          case None => Source.empty
        }
      }

    // Note that .alsoTo triggers after .to, see https://stackoverflow.com/questions/47895991/multiple-sinks-in-the-same-stream#comment93028098_47896071
    @nowarn("msg=method lazyInit in object Sink is deprecated")
    val subFlowSink = substreams
      .alsoTo(kafkaClientInterface.commitCursor.contramap[((ByteString, kafkaClientInterface.CursorContext), String)] {
        case ((_, context), _) => context
      })
      .to(
        // See https://stackoverflow.com/questions/68774425/combine-prefixandtail1-with-sink-lazysink-for-subflow-created-by-splitafter/68776660?noredirect=1#comment121558518_68776660
        Sink.lazyInit(
          { case (_, key) =>
            Future.successful(
              backupToStorageSink(key).contramap[((ByteString, kafkaClientInterface.CursorContext), String)] {
                case ((byteString, _), _) => byteString
              }
            )
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

  /** Calculates the current position in 2 element sliding of a Stream.
    * @param dividedPeriodsBefore
    *   The number of divided periods in the first element of the slide. -1 is used as a sentinel value to indicate the
    *   start of the stream
    * @param dividedPeriodsAfter
    *   The number of divided periods in the second element of the slide
    * @return
    *   The position of the Stream
    */
  def splitAtBoundaryCondition(dividedPeriodsBefore: Long, dividedPeriodsAfter: Long): BackupStreamPosition =
    (dividedPeriodsBefore, dividedPeriodsAfter) match {
      case (before, after) if after > before =>
        BackupStreamPosition.Boundary
      case _ =>
        BackupStreamPosition.Middle
    }

  /** Transforms a `ReducedConsumer` record into a ByteString so that it can be persisted into the data storage
    * @param reducedConsumerRecord
    *   The ReducedConsumerRecord to persist
    * @param backupStreamPosition
    *   The position of the record relative in the stream (so it knows if its at the start, middle or end)
    * @return
    *   a `ByteString` ready to be persisted
    */
  def transformReducedConsumerRecords(reducedConsumerRecord: ReducedConsumerRecord,
                                      backupStreamPosition: BackupStreamPosition
  ): ByteString = {
    val string = backupStreamPosition match {
      case BackupStreamPosition.Start =>
        s"[${reducedConsumerRecordAsString(reducedConsumerRecord)},"
      case BackupStreamPosition.Middle =>
        s"${reducedConsumerRecordAsString(reducedConsumerRecord)},"
      case BackupStreamPosition.Boundary =>
        s"${reducedConsumerRecordAsString(reducedConsumerRecord)}]"
    }
    ByteString(string)
  }

  protected def calculateNumberOfPeriodsFromTimestamp(initialTime: OffsetDateTime,
                                                      period: FiniteDuration,
                                                      reducedConsumerRecord: ReducedConsumerRecord
  ): Long =
    // TODO handle overflow?
    ChronoUnit.MICROS.between(initialTime, reducedConsumerRecord.toOffsetDateTime) / period.toMicros
}
