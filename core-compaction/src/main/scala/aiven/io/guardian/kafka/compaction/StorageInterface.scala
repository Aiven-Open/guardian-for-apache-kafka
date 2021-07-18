package aiven.io.guardian.kafka.compaction

import aiven.io.guardian.kafka.models.KafkaRow
import akka.NotUsed
import akka.stream.scaladsl.Source

trait StorageInterface {

  /** Retrieve Kafka data from a given storage source
    * @return
    */
  def retrieveKafkaData: Source[KafkaRow, NotUsed]

  /** Checks whether the storage exists and is accessible
    */
  def checkStorageAccessible: Source[Boolean, NotUsed]
}
