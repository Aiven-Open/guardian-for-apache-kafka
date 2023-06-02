package io.aiven.guardian.kafka.compaction

import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import org.apache.pekko

import pekko.NotUsed
import pekko.stream.scaladsl.Source

trait StorageInterface {

  /** Retrieve Kafka data from a given storage source
    * @return
    */
  def retrieveKafkaData: Source[ReducedConsumerRecord, NotUsed]

  /** Checks whether the storage exists and is accessible
    */
  def checkStorageAccessible: Source[Boolean, NotUsed]
}
