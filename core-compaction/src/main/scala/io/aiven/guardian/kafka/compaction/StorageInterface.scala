package io.aiven.guardian.kafka.compaction

import akka.NotUsed
import akka.stream.scaladsl.Source
import io.aiven.guardian.kafka.models.ReducedConsumerRecord

trait StorageInterface {

  /** Retrieve Kafka data from a given storage source
    * @return
    */
  def retrieveKafkaData: Source[ReducedConsumerRecord, NotUsed]

  /** Checks whether the storage exists and is accessible
    */
  def checkStorageAccessible: Source[Boolean, NotUsed]
}
