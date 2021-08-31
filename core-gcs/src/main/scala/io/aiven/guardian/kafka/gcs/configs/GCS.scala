package io.aiven.guardian.kafka.gcs.configs

/** GCS specific configuration used when storing Kafka ConsumerRecords to a GCS bucket
  * @param dataBucket
  *   The bucket where a Kafka Consumer directly streams data into as storage
  * @param compactionBucket
  *   The bucket where compaction results are stored
  */
final case class GCS(dataBucket: String, compactionBucket: String)
