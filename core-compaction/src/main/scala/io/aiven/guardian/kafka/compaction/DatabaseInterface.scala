package io.aiven.guardian.kafka.compaction

import scala.concurrent.Future

import akka.NotUsed
import akka.stream.javadsl.Flow
import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.aiven.guardian.kafka.models.ReducedConsumerRecord

trait DatabaseInterface {

  /** Given a source of storage where Kafka messages are contained, stream it into a database.
    * @param kafkaStorageSource
    * @param encodeKafkaRowToByteString
    * @return
    *   Number of rows updated
    */
  def streamInsert(kafkaStorageSource: Source[ReducedConsumerRecord, NotUsed],
                   encodeKafkaRowToByteString: Flow[ReducedConsumerRecord, ByteString, NotUsed]
  ): Future[Long]
}
