package aiven.io.guardian.kafka.compaction

import aiven.io.guardian.kafka.models.KafkaRow
import akka.NotUsed
import akka.stream.javadsl.Flow
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.concurrent.Future

trait DatabaseInterface {

  /** Given a source of storage where Kafka messages are contained, stream it into a database.
    * @param kafkaStorageSource
    * @param encodeKafkaRowToByteString
    * @return Number of rows updated
    */
  def streamInsert(kafkaStorageSource: Source[KafkaRow, NotUsed],
                   encodeKafkaRowToByteString: Flow[KafkaRow, ByteString, NotUsed]
  ): Future[Long]
}
