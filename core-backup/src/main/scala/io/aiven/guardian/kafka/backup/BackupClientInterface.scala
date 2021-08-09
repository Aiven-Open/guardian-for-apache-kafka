package io.aiven.guardian.kafka.backup

import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.codecs.Circe._
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.kafka.{Errors, KafkaClientInterface}
import akka.Done
import akka.stream.FlowShape
import akka.stream.scaladsl._
import akka.util.ByteString
import io.circe.syntax._

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal._
import scala.annotation.nowarn
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

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
  */
trait BackupClientInterface {
  implicit val kafkaClientInterface: KafkaClientInterface
  implicit val backupConfig: Backup

  /** Override this type to define the result of backing up data to a datasource
    */
  type BackupResult

  import BackupClientInterface._

  /** Override this method to define how to backup a `ByteString` to a `DataSource`
    * @param key The object key or filename for what is being backed up
    * @return A Sink that also provides a `BackupResult`
    */
  def backupToStorageSink(key: String): Sink[ByteString, Future[BackupResult]]

  /** A Flow that both backs up the `ByteString` data to a data source and then
    * commits the Kafka `CursorContext` using `kafkaClientInterface.commitCursor`.
    * @param key They object key or filename for what is being backed up
    * @return The `CursorContext` which can be used for logging/debugging
    */
  def backupAndCommitFlow(
      key: String
  ): Flow[(ByteString, kafkaClientInterface.CursorContext), kafkaClientInterface.CursorContext, Future[Done]] = {
    val sink = Flow.fromGraph(
      GraphDSL.create(
        backupToStorageSink(key),
        kafkaClientInterface.commitCursor
      )((_, cursorCommitted) => cursorCommitted)(implicit builder =>
        (backupSink, commitCursor) => {
          import GraphDSL.Implicits._

          val b = builder.add(Concat[(ByteString, kafkaClientInterface.CursorContext)]())

          b.out.map(_._1) ~> backupSink
          b.out.map(_._2) ~> commitCursor

          new FlowShape(b.in(0), b.out)
        }
      )
    )
    sink.map { case (_, context) => context }
  }

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

  private[backup] val sourceWithPeriods: SourceWithContext[(ReducedConsumerRecord, Long),
                                                           kafkaClientInterface.CursorContext,
                                                           kafkaClientInterface.Control
  ] =
    SourceWithContext.fromTuples(
      kafkaClientInterface.getSource.asSource.prefixAndTail(1).flatMapConcat { case (head, _) =>
        head.headOption match {
          case Some((firstReducedConsumerRecord, _)) =>
            kafkaClientInterface.getSource.map { reducedConsumerRecord =>
              val period =
                calculateNumberOfPeriodsFromTimestamp(firstReducedConsumerRecord.toOffsetDateTime,
                                                      backupConfig.periodSlice,
                                                      reducedConsumerRecord
                )
              (reducedConsumerRecord, period)
            }

          case None => throw Errors.ExpectedStartOfSource
        }
      }
    )

  /** The entire flow that involves reading from Kafka, transforming the data into JSON and then backing it up into
    * a data source.
    * @return The `CursorContext` which can be used for logging/debugging along with the `kafkaClientInterface.Control`
    *         which can be used to control the Stream
    */
  protected def backup: Source[kafkaClientInterface.CursorContext, kafkaClientInterface.Control] = {
    // TODO use https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3api/list-multipart-uploads.html
    // and https://stackoverflow.com/questions/53764876/resume-s3-multipart-upload-partetag to find any in progress
    // multiupload to resume from previous termination. Looks like we will have to do this manually since its not in
    // Alpakka yet
    val withBackupStreamPositions = calculateBackupStreamPositions(sourceWithPeriods)

    val split = withBackupStreamPositions.asSource.splitAfter { case ((_, backupStreamPosition), _) =>
      backupStreamPosition == BackupStreamPosition.Boundary
    }

    split
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

            transformed.via(backupAndCommitFlow(key))
          case None => throw Errors.ExpectedStartOfSource
        }
      }
      .mergeSubstreams
  }
}

object BackupClientInterface {
  def reducedConsumerRecordAsString(reducedConsumerRecord: ReducedConsumerRecord): String =
    io.circe.Printer.noSpaces.print(reducedConsumerRecord.asJson)

  def formatOffsetDateTime(offsetDateTime: OffsetDateTime): String =
    offsetDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

  /** Calculate an object storage key or filename for a ReducedConsumerRecord
    * @param offsetDateTime A given time
    * @return A `String` that can be used either as some object key or a filename
    */
  def calculateKey(offsetDateTime: OffsetDateTime): String =
    s"${BackupClientInterface.formatOffsetDateTime(offsetDateTime)}.json"

  /** Calculates the current position in 2 element sliding of a Stream.
    * @param dividedPeriodsBefore The number of divided periods in the first element of the slide. -1 is used as a
    *                             sentinel value to indicate the start of the stream
    * @param dividedPeriodsAfter The number of divided periods in the second element of the slide
    * @return The position of the Stream
    */
  def splitAtBoundaryCondition(dividedPeriodsBefore: Long, dividedPeriodsAfter: Long): BackupStreamPosition =
    (dividedPeriodsBefore, dividedPeriodsAfter) match {
      case (before, after) if after > before =>
        BackupStreamPosition.Boundary
      case _ =>
        BackupStreamPosition.Middle
    }

  /** Transforms a `ReducedConsumer` record into a ByteString so that it can be persisted into the data storage
    * @param reducedConsumerRecord The ReducedConsumerRecord to persist
    * @param backupStreamPosition The position of the record relative in the stream (so it knows if its at the start,
    *                             middle or end)
    * @return a `ByteString` ready to be persisted
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
