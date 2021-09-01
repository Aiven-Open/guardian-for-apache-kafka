package io.aiven.guardian.kafka.compaction

import akka.NotUsed
import akka.stream.ActorAttributes
import akka.stream.Materializer
import akka.stream.javadsl.Flow
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.StreamConverters
import akka.util.ByteString
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking

import java.sql.Connection

/** A Postgres Database backed by JDBC which uses the Postgres COPY command to insert data into the database. Note that
  * since this uses JDBC and CopyManager, its implementation is blocking under the hood.
  * @param scheduler
  * @param materializer
  * @param conn
  */
class PostgresJDBCDatabase()(implicit executionContext: ExecutionContext, materializer: Materializer, conn: Connection)
    extends DatabaseInterface {

  /** Inserts data into a Postgres Database using the COPY method (see
    * https://www.postgresql.org/docs/9.4/sql-copy.html). This means the data insertion is buffered and also extremely
    * fast since it bypasses internal parts of the Postgres engine which are not necessary.
    *
    * Since it uses JDBC plus `java.io.InputStream` under the hood, the operation is inherently blocking even though it
    * returns a `scala.concurrent.Future`. Due to this we have used blocking IO dispatchers to avoid problems that are
    * typical of blocking IO
    *
    * @return
    *   Number of rows updated
    */
  override def streamInsert(kafkaStorageSource: Source[ReducedConsumerRecord, NotUsed],
                            encodeKafkaRowToByteString: Flow[ReducedConsumerRecord, ByteString, NotUsed]
  ): Future[Long] = {
    // TODO implement SQL query
    val sql = """"""

    // Since this is blocking IO we use a custom dispatcher dealt to handle with this
    val sink = StreamConverters
      .asInputStream()
      .withAttributes(ActorAttributes.dispatcher(ActorAttributes.IODispatcher.dispatcher))

    val postgresSource = kafkaStorageSource.via(encodeKafkaRowToByteString)

    blocking(Future {
      postgresSource.runWith(
        sink.mapMaterializedValue(inputStream =>
          new CopyManager(conn.asInstanceOf[BaseConnection]).copyIn(
            sql,
            inputStream
          )
        )
      )
    })
  }

}
