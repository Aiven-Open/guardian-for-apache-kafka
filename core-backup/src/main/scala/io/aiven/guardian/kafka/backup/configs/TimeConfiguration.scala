package io.aiven.guardian.kafka.backup.configs

import scala.concurrent.duration.FiniteDuration

import java.time.temporal.ChronoUnit

sealed trait TimeConfiguration

/** Backs up objects/files depending on the timestamp fo the first received Kafka message. Suspending/resuming the
  * backup client will always create a new object/file
  * @param duration
  *   The maximum span of time for each object/file, when this duration is exceeded a new file is created
  */
final case class PeriodFromFirst(duration: FiniteDuration) extends TimeConfiguration

/** Backs up objects/files by collecting received Kafka messages into a single time slice based on a
  * [[java.time.temporal.ChronoUnit]]. When suspending/resuming the backup client, this option will reuse existing
  * objects/files if they fall into the currently configured `chronoUnit`.
  * @param chronoUnit
  *   Timestamps for kafka messages that are contained within the configured [[java.time.temporal.ChronoUnit]] will be
  *   placed into the same object/file.
  */
final case class ChronoUnitSlice(chronoUnit: ChronoUnit) extends TimeConfiguration
