package io.aiven.guardian.kafka.backup.configs

/** @param timeConfiguration
  *   Determines how the backed up objects/files are segregated depending on a time configuration
  */
final case class Backup(timeConfiguration: TimeConfiguration)
