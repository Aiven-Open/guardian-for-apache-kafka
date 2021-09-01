package io.aiven.guardian.kafka.compaction.s3

import akka.NotUsed
import akka.stream.alpakka.s3.BucketAccess
import akka.stream.alpakka.s3.S3Headers
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.StrictLogging
import io.aiven.guardian.kafka.compaction.StorageInterface
import io.aiven.guardian.kafka.compaction.s3.models.StorageConfig
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.kafka.s3.errors.S3Errors

import scala.annotation.nowarn

class StorageClient(bucketName: String, prefix: Option[String], s3Headers: S3Headers)(implicit
    storageConfig: StorageConfig
) extends StorageInterface
    with StrictLogging {

  /** Retrieve Kafka data from a given storage source
    *
    * @return
    */
  @throws(classOf[S3Errors.ExpectedObjectToExist])
  override def retrieveKafkaData: Source[ReducedConsumerRecord, NotUsed] = {
    // TODO filter the correct buckets to retrieve
    @nowarn("msg=is never used")
    val byteStringSource = S3
      .listBucket(bucketName, prefix, s3Headers)
      .flatMapMerge(
        storageConfig.parallelObjectDownloadLimit,
        bucketDetails =>
          S3.download(bucketName, bucketDetails.key, None, None, s3Headers)
            .map(
              _.getOrElse(
                throw S3Errors.ExpectedObjectToExist(bucketName, bucketDetails.key, None, None, s3Headers)
              )
            )
      )

    // TODO serialization from raw bytes to Kafka Topic Format
    ???
  }

  /** Checks whether the storage exists and is accessible
    */
  def checkStorageAccessible: Source[Boolean, NotUsed] =
    S3.checkIfBucketExistsSource(bucketName, s3Headers).map {
      case e @ (BucketAccess.AccessDenied | BucketAccess.NotExists) =>
        logger.error(s"Accessing S3 $bucketName gave ${e.toString}")
        false
      case BucketAccess.AccessGranted =>
        logger.info(s"Successfully accessed S3 $bucketName")
        true
    }

}
