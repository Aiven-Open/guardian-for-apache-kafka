package io.aiven.guardian.kafka.compaction.s3

import com.typesafe.scalalogging.LazyLogging
import io.aiven.guardian.kafka.compaction.StorageInterface
import io.aiven.guardian.kafka.compaction.s3.models.StorageConfig
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.kafka.s3.errors.S3Errors
import org.apache.pekko

import scala.annotation.nowarn

import pekko.NotUsed
import pekko.stream.connectors.s3.BucketAccess
import pekko.stream.connectors.s3.S3Headers
import pekko.stream.connectors.s3.scaladsl.S3
import pekko.stream.scaladsl.Source

class StorageClient(bucketName: String, prefix: Option[String], s3Headers: S3Headers)(implicit
    storageConfig: StorageConfig
) extends StorageInterface
    with LazyLogging {

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
        bucketDetails => S3.getObject(bucketName, bucketDetails.key, None, None, s3Headers)
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
