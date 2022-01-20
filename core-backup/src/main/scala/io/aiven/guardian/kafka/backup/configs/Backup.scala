package io.aiven.guardian.kafka.backup.configs

/** @param kafkaGroupId
  *   The group-id that the Kafka consumer will use
  * @param timeConfiguration
  *   Determines how the backed up objects/files are segregated depending on a time configuration
  */
final case class Backup(kafkaGroupId: String, timeConfiguration: TimeConfiguration)
