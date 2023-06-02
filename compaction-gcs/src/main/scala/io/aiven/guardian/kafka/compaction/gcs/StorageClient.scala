package io.aiven.guardian.kafka.compaction.gcs

import com.typesafe.scalalogging.LazyLogging
import io.aiven.guardian.kafka.compaction.StorageInterface
import io.aiven.guardian.kafka.compaction.gcs.models.StorageConfig
import io.aiven.guardian.kafka.gcs.errors.GCSErrors
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import org.apache.pekko

import scala.annotation.nowarn

import pekko.NotUsed
import pekko.stream.connectors.googlecloud.storage.scaladsl.GCStorage
import pekko.stream.scaladsl.Source

class StorageClient(bucketName: String, maybePrefix: Option[String])(implicit storageConfig: StorageConfig)
    extends StorageInterface
    with LazyLogging {

  /** Retrieve Kafka data from a given storage source
    *
    * @return
    */
  @throws(classOf[GCSErrors.ExpectedObjectToExist])
  override def retrieveKafkaData: Source[ReducedConsumerRecord, NotUsed] = {

    @nowarn("msg=is never used")
    // TODO filter the correct buckets to retrieve
    val byteStringSource = GCStorage
      .listBucket(bucketName, maybePrefix, versions = false)
      .flatMapMerge(
        storageConfig.parallelObjectDownloadLimit,
        storageObject =>
          GCStorage
            .download(bucketName, storageObject.name)
            .map(
              _.getOrElse(
                throw GCSErrors.ExpectedObjectToExist(bucketName, maybePrefix)
              )
            )
      )

    // TODO serialization from raw bytes to Kafka Topic Format
    ???
  }

  /** Checks whether the storage exists and is accessible
    */
  def checkStorageAccessible: Source[Boolean, NotUsed] =
    GCStorage.getBucketSource(bucketName).map(_.isDefined).map {
      case false =>
        logger.error(s"Failed accessing GCS $bucketName")
        false
      case true =>
        logger.info(s"Successfully accessed GCS $bucketName")
        true
    }
}
