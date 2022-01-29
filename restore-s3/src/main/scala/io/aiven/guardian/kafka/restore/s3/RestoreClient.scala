package io.aiven.guardian.kafka.restore.s3

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.s3.S3Attributes
import akka.stream.alpakka.s3.S3Headers
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import io.aiven.guardian.kafka.configs.KafkaCluster
import io.aiven.guardian.kafka.restore.KafkaProducerInterface
import io.aiven.guardian.kafka.restore.RestoreClientInterface
import io.aiven.guardian.kafka.restore.configs.Restore
import io.aiven.guardian.kafka.s3.configs.{S3 => S3Config}

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

class RestoreClient[T <: KafkaProducerInterface](maybeS3Settings: Option[S3Settings])(implicit
    override val kafkaProducerInterface: T,
    override val restoreConfig: Restore,
    override val kafkaClusterConfig: KafkaCluster,
    override val system: ActorSystem,
    s3Config: S3Config,
    s3Headers: S3Headers
) extends RestoreClientInterface[T] {

  override def retrieveBackupKeys: Future[List[String]] = {
    implicit val ec: ExecutionContext = system.dispatcher

    val base = S3.listBucket(s3Config.dataBucket, s3Config.dataBucketPrefix, s3Headers)
    for {
      bucketContents <- maybeS3Settings
                          .fold(base)(s3Settings => base.withAttributes(S3Attributes.settings(s3Settings)))
                          .runWith(Sink.collection)
    } yield bucketContents.map(_.key).toList
  }

  override def downloadFlow: Flow[String, ByteString, NotUsed] =
    Flow[String]
      .flatMapConcat { key =>
        val base = S3.download(s3Config.dataBucket, key, None, None, s3Headers)
        maybeS3Settings
          .fold(base)(s3Settings => base.withAttributes(S3Attributes.settings(s3Settings)))
          .collect { case Some(value) =>
            value
          }
          .flatMapConcat { case (source, _) => source }
      }

}
