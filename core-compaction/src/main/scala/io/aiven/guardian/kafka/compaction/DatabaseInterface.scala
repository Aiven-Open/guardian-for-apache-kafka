package io.aiven.guardian.kafka.compaction

import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import org.apache.pekko

import scala.concurrent.Future

import pekko.NotUsed
import pekko.stream.javadsl.Flow
import pekko.stream.scaladsl.Source
import pekko.util.ByteString

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
