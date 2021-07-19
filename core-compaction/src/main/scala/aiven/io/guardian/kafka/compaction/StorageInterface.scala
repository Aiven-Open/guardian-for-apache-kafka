package aiven.io.guardian.kafka.compaction

import aiven.io.guardian.kafka.models.ReducedConsumerRecord
import akka.NotUsed
import akka.stream.scaladsl.Source

trait StorageInterface {

  /** Retrieve Kafka data from a given storage source
    * @return
    */
  def retrieveKafkaData: Source[ReducedConsumerRecord, NotUsed]

  /** Checks whether the storage exists and is accessible
    */
  def checkStorageAccessible: Source[Boolean, NotUsed]
}
