package aiven.io.guardian.kafka.compaction.gcs

import aiven.io.guardian.kafka.compaction.StorageInterface
import aiven.io.guardian.kafka.compaction.gcs.models.StorageConfig
import aiven.io.guardian.kafka.gcs.errors.GCSErrors
import aiven.io.guardian.kafka.models.KafkaRow
import akka.NotUsed
import akka.stream.alpakka.googlecloud.storage.scaladsl.GCStorage
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.StrictLogging

class StorageClient(bucketName: String, maybePrefix: Option[String])(implicit storageConfig: StorageConfig)
    extends StorageInterface
    with StrictLogging {

  /** Retrieve Kafka data from a given storage source
    *
    * @return
    */
  @throws(classOf[GCSErrors.ExpectedObjectToExist])
  override def retrieveKafkaData: Source[KafkaRow, NotUsed] = {

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
