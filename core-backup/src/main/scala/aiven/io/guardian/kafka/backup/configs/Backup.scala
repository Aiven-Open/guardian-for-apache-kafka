package aiven.io.guardian.kafka.backup.configs

import scala.concurrent.duration.FiniteDuration

/** @param periodSlice The time period for each given slice that stores all of the `ReducedConsumerRecord`
  */
final case class Backup(periodSlice: FiniteDuration)
