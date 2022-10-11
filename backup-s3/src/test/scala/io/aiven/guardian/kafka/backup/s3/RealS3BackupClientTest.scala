package io.aiven.guardian.kafka.backup.s3

import akka.kafka.scaladsl.Producer
import akka.stream.KillSwitches
import akka.stream.SharedKillSwitch
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.Compression
import akka.stream.scaladsl.Sink
import com.softwaremill.diffx.scalatest.DiffMustMatcher._
import io.aiven.guardian.kafka.Generators._
import io.aiven.guardian.kafka.KafkaClusterTest
import io.aiven.guardian.kafka.TestUtils._
import io.aiven.guardian.kafka.Utils
import io.aiven.guardian.kafka.backup.BackupClientControlWrapper
import io.aiven.guardian.kafka.backup.KafkaClient
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.backup.configs.ChronoUnitSlice
import io.aiven.guardian.kafka.backup.configs.PeriodFromFirst
import io.aiven.guardian.kafka.backup.configs.{Compression => CompressionConfig}
import io.aiven.guardian.kafka.codecs.Circe._
import io.aiven.guardian.kafka.configs.KafkaCluster
import io.aiven.guardian.kafka.models.Gzip
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.kafka.s3.Generators.s3ConfigGen
import io.aiven.guardian.kafka.s3.configs.{S3 => S3Config}
import org.mdedetrich.akka.stream.support.CirceStreamSupport
import org.scalatest.propspec.AnyPropSpecLike

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

import java.time.temporal.ChronoUnit

trait RealS3BackupClientTest extends AnyPropSpecLike with KafkaClusterTest with BackupClientSpec {
  def compression: Option[CompressionConfig]

  override lazy val s3Settings: S3Settings = S3Settings()

  /** Virtual Dot Host in bucket names are disabled because you need an actual DNS certificate otherwise AWS will fail
    * on bucket creation
    */
  override lazy val useVirtualDotHost: Boolean            = false
  override lazy val bucketPrefix: Option[String]          = Some("guardian-")
  override lazy val enableCleanup: Option[FiniteDuration] = Some(5 seconds)

  def createKafkaClient(
      killSwitch: SharedKillSwitch
  )(implicit kafkaClusterConfig: KafkaCluster, backupConfig: Backup): KafkaClientWithKillSwitch =
    new KafkaClientWithKillSwitch(
      configureConsumer = baseKafkaConfig,
      killSwitch = killSwitch
    )

  def getKeyFromSingleDownload(dataBucket: String): Future[String] = waitForS3Download(
    dataBucket,
    {
      case Seq(single) => single.key
      case rest =>
        throw DownloadNotReady(rest)
    }
  )

  def getKeysFromTwoDownloads(dataBucket: String): Future[(String, String)] = waitForS3Download(
    dataBucket,
    {
      case Seq(first, second) => (first.key, second.key)
      case rest =>
        throw DownloadNotReady(rest)
    }
  )

  def waitUntilBackupClientHasCommitted(backupClient: BackupClientChunkState[_],
                                        step: FiniteDuration = 100 millis,
                                        delay: FiniteDuration = 5 seconds
  ): Future[Unit] =
    if (backupClient.processedChunks.size() > 0)
      akka.pattern.after(delay)(Future.successful(()))
    else
      akka.pattern.after(step)(waitUntilBackupClientHasCommitted(backupClient, step, delay))

  private def downloadObject(dataBucket: String, key: String) = {
    val downloadSource = S3
      .getObject(dataBucket, key)
      .withAttributes(s3Attrs)

    val decode = CirceStreamSupport.decode[List[Option[ReducedConsumerRecord]]]
    (compression match {
      case Some(CompressionConfig(Gzip, _)) => downloadSource.via(Compression.gunzip()).via(decode)
      case None                             => downloadSource.via(decode)
    }).runWith(Sink.seq)
  }

  property("basic flow without interruptions using PeriodFromFirst works correctly", RealS3Available) {
    forAll(kafkaDataWithMinByteSizeGen(S3.MinChunkSize, 2, reducedConsumerRecordsToJson),
           s3ConfigGen(useVirtualDotHost, bucketPrefix),
           kafkaConsumerGroupGen
    ) {
      (kafkaDataInChunksWithTimePeriod: KafkaDataInChunksWithTimePeriod,
       s3Config: S3Config,
       kafkaConsumerGroup: String
      ) =>
        logger.info(s"Data bucket is ${s3Config.dataBucket}")

        val data = kafkaDataInChunksWithTimePeriod.data.flatten

        val topics = data.map(_.topic).toSet

        val asProducerRecords = toProducerRecords(data)
        val baseSource        = toSource(asProducerRecords, 30 seconds)

        implicit val kafkaClusterConfig: KafkaCluster = KafkaCluster(topics)

        implicit val config: S3Config = s3Config
        implicit val backupConfig: Backup =
          Backup(kafkaConsumerGroup, PeriodFromFirst(1 minute), 10 seconds, compression)

        val producerSettings = createProducer()

        val backupClientWrapped =
          new BackupClientControlWrapper(
            new BackupClient(Some(s3Settings))(new KafkaClient(configureConsumer = baseKafkaConfig),
                                               implicitly,
                                               implicitly,
                                               implicitly,
                                               implicitly
            )
          )

        val calculatedFuture = for {
          _ <- createTopics(topics)
          _ <- createBucket(s3Config.dataBucket)
          _ = backupClientWrapped.run()
          _ <- akka.pattern.after(KafkaInitializationTimeoutConstant)(
                 baseSource
                   .runWith(Producer.plainSink(producerSettings))
               )
          _          <- sendTopicAfterTimePeriod(1 minute, producerSettings, topics.head)
          key        <- getKeyFromSingleDownload(s3Config.dataBucket)
          downloaded <- downloadObject(s3Config.dataBucket, key)
        } yield downloaded.toList.flatten.collect { case Some(reducedConsumerRecord) =>
          reducedConsumerRecord
        }

        calculatedFuture.onComplete { _ =>
          cleanTopics(topics)
          backupClientWrapped.shutdown()
        }
        val downloaded = calculatedFuture.futureValue

        val downloadedGroupedAsKey = downloaded
          .groupBy(_.key)
          .view
          .mapValues { reducedConsumerRecords =>
            reducedConsumerRecords.map(_.value)
          }
          .toMap

        val inputAsKey = data
          .groupBy(_.key)
          .view
          .mapValues { reducedConsumerRecords =>
            reducedConsumerRecords.map(_.value)
          }
          .toMap

        downloadedGroupedAsKey mustMatchTo inputAsKey
    }
  }

  property("suspend/resume using PeriodFromFirst creates separate object after resume point", RealS3Available) {
    forAll(kafkaDataWithMinByteSizeGen(S3.MinChunkSize, 2, reducedConsumerRecordsToJson),
           s3ConfigGen(useVirtualDotHost, bucketPrefix),
           kafkaConsumerGroupGen
    ) {
      (kafkaDataInChunksWithTimePeriod: KafkaDataInChunksWithTimePeriod,
       s3Config: S3Config,
       kafkaConsumerGroup: String
      ) =>
        logger.info(s"Data bucket is ${s3Config.dataBucket}")

        val data = kafkaDataInChunksWithTimePeriod.data.flatten

        val topics = data.map(_.topic).toSet

        implicit val kafkaClusterConfig: KafkaCluster = KafkaCluster(topics)

        implicit val config: S3Config = s3Config
        implicit val backupConfig: Backup =
          Backup(kafkaConsumerGroup, PeriodFromFirst(1 minute), 10 seconds, compression)

        val producerSettings = createProducer()

        val killSwitch = KillSwitches.shared("kill-switch")

        val backupClient =
          new BackupClientChunkState(Some(s3Settings))(createKafkaClient(killSwitch),
                                                       implicitly,
                                                       implicitly,
                                                       implicitly,
                                                       implicitly
          )
        val backupClientWrapped = new BackupClientControlWrapper(backupClient)

        val secondBackupClient = new BackupClient(Some(s3Settings))(
          new KafkaClient(configureConsumer = baseKafkaConfig),
          implicitly,
          implicitly,
          implicitly,
          implicitly
        )
        val secondBackupClientWrapped = new BackupClientControlWrapper(secondBackupClient)

        val asProducerRecords = toProducerRecords(data)
        val baseSource        = toSource(asProducerRecords, 30 seconds)

        val calculatedFuture = for {
          _ <- createTopics(topics)
          _ <- createBucket(s3Config.dataBucket)
          _ = backupClientWrapped.run()
          _ = baseSource.runWith(Producer.plainSink(producerSettings))
          _ <- waitUntilBackupClientHasCommitted(backupClient)
          _ = killSwitch.abort(TerminationException)
          _ <- akka.pattern.after(2 seconds) {
                 Future {
                   secondBackupClientWrapped.run()
                 }
               }
          _                     <- sendTopicAfterTimePeriod(1 minute, producerSettings, topics.head)
          (firstKey, secondKey) <- getKeysFromTwoDownloads(s3Config.dataBucket)
          firstDownloadedFuture  = downloadObject(s3Config.dataBucket, firstKey)
          secondDownloadedFuture = downloadObject(s3Config.dataBucket, secondKey)
          firstDownloaded  <- firstDownloadedFuture
          secondDownloaded <- secondDownloadedFuture
        } yield {
          val first = firstDownloaded.toList.flatten.collect { case Some(reducedConsumerRecord) =>
            reducedConsumerRecord
          }

          val second = secondDownloaded.toList.flatten.collect { case Some(reducedConsumerRecord) =>
            reducedConsumerRecord
          }
          (first, second)
        }

        calculatedFuture.onComplete { _ =>
          cleanTopics(topics)
          backupClientWrapped.shutdown()
          secondBackupClientWrapped.shutdown()
        }
        val (firstDownloaded, secondDownloaded) = calculatedFuture.futureValue

        // Only care about ordering when it comes to key
        val firstDownloadedGroupedAsKey = firstDownloaded
          .groupBy(_.key)
          .view
          .mapValues { reducedConsumerRecords =>
            reducedConsumerRecords.map(_.value)
          }
          .toMap

        val secondDownloadedGroupedAsKey = secondDownloaded
          .groupBy(_.key)
          .view
          .mapValues { reducedConsumerRecords =>
            reducedConsumerRecords.map(_.value)
          }
          .toMap

        val inputAsKey = data
          .groupBy(_.key)
          .view
          .mapValues { reducedConsumerRecords =>
            reducedConsumerRecords.map(_.value)
          }
          .toMap

        val downloaded = (firstDownloadedGroupedAsKey.keySet ++ secondDownloadedGroupedAsKey.keySet).map { key =>
          (key,
           firstDownloadedGroupedAsKey.getOrElse(key, List.empty) ++ secondDownloadedGroupedAsKey.getOrElse(key,
                                                                                                            List.empty
           )
          )
        }.toMap

        downloaded mustMatchTo inputAsKey
    }
  }

  property("suspend/resume for same object using ChronoUnitSlice works correctly", RealS3Available) {
    forAll(kafkaDataWithMinByteSizeGen(S3.MinChunkSize, 2, reducedConsumerRecordsToJson),
           s3ConfigGen(useVirtualDotHost, bucketPrefix),
           kafkaConsumerGroupGen
    ) {
      (kafkaDataInChunksWithTimePeriod: KafkaDataInChunksWithTimePeriod,
       s3Config: S3Config,
       kafkaConsumerGroup: String
      ) =>
        logger.info(s"Data bucket is ${s3Config.dataBucket}")

        val data = kafkaDataInChunksWithTimePeriod.data.flatten

        val topics = data.map(_.topic).toSet

        implicit val kafkaClusterConfig: KafkaCluster = KafkaCluster(topics)

        implicit val config: S3Config = s3Config
        implicit val backupConfig: Backup =
          Backup(kafkaConsumerGroup, ChronoUnitSlice(ChronoUnit.MINUTES), 10 seconds, compression)

        val producerSettings = createProducer()

        val killSwitch = KillSwitches.shared("kill-switch")

        val backupClient =
          new BackupClientChunkState(Some(s3Settings))(createKafkaClient(killSwitch),
                                                       implicitly,
                                                       implicitly,
                                                       implicitly,
                                                       implicitly
          )
        val backupClientWrapped = new BackupClientControlWrapper(backupClient)

        val secondBackupClient =
          new BackupClient(Some(s3Settings))(
            new KafkaClient(configureConsumer = baseKafkaConfig),
            implicitly,
            implicitly,
            implicitly,
            implicitly
          )
        val secondBackupClientWrapped = new BackupClientControlWrapper(secondBackupClient)

        val asProducerRecords = toProducerRecords(data)
        val baseSource        = toSource(asProducerRecords, 30 seconds)

        val calculatedFuture = for {
          _ <- createTopics(topics)
          _ <- createBucket(s3Config.dataBucket)
          _ = backupClientWrapped.run()
          _ <- waitForStartOfTimeUnit(ChronoUnit.MINUTES)
          _ = baseSource.runWith(Producer.plainSink(producerSettings))
          _ <- waitUntilBackupClientHasCommitted(backupClient)
          _ = killSwitch.abort(TerminationException)
          _ <- akka.pattern.after(2 seconds) {
                 Future {
                   secondBackupClientWrapped.run()
                 }
               }
          _          <- sendTopicAfterTimePeriod(1 minute, producerSettings, topics.head)
          key        <- getKeyFromSingleDownload(s3Config.dataBucket)
          downloaded <- downloadObject(s3Config.dataBucket, key)
        } yield downloaded.toList.flatten.collect { case Some(reducedConsumerRecord) =>
          reducedConsumerRecord
        }

        calculatedFuture.onComplete { _ =>
          cleanTopics(topics)
          backupClientWrapped.shutdown()
          secondBackupClientWrapped.shutdown()
        }
        val downloaded = calculatedFuture.futureValue

        // Only care about ordering when it comes to key
        val downloadedGroupedAsKey = downloaded
          .groupBy(_.key)
          .view
          .mapValues { reducedConsumerRecords =>
            reducedConsumerRecords.map(_.value)
          }
          .toMap

        val inputAsKey = data
          .groupBy(_.key)
          .view
          .mapValues { reducedConsumerRecords =>
            reducedConsumerRecords.map(_.value)
          }
          .toMap

        downloadedGroupedAsKey mustMatchTo inputAsKey
    }
  }

  property(
    "Backup works with multiple keys",
    RealS3Available
  ) {
    forAll(kafkaDataWithTimePeriodsGen(min = 30000, max = 30000),
           s3ConfigGen(useVirtualDotHost, bucketPrefix),
           kafkaConsumerGroupGen
    ) { (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod, s3Config: S3Config, kafkaConsumerGroup: String) =>
      logger.info(s"Data bucket is ${s3Config.dataBucket}")
      val data = kafkaDataWithTimePeriod.data

      val topics = data.map(_.topic).toSet

      val asProducerRecords = toProducerRecords(data)
      val baseSource        = toSource(asProducerRecords, 30 seconds)

      implicit val kafkaClusterConfig: KafkaCluster = KafkaCluster(topics)

      val producerSettings = createProducer()

      implicit val config: S3Config = s3Config

      implicit val backupConfig: Backup =
        Backup(kafkaConsumerGroup, PeriodFromFirst(1 second), 10 seconds, compression)
      val backupClientWrapped =
        new BackupClientControlWrapper(
          new BackupClient(Some(s3Settings))(new KafkaClient(configureConsumer = baseKafkaConfig),
                                             implicitly,
                                             implicitly,
                                             implicitly,
                                             implicitly
          )
        )

      val calculatedFuture = for {
        _ <- createTopics(topics)
        _ <- createBucket(s3Config.dataBucket)
        _ = backupClientWrapped.run()
        _ <- akka.pattern.after(KafkaInitializationTimeoutConstant)(
               baseSource
                 .runWith(Producer.plainSink(producerSettings))
             )

        _ <- sendTopicAfterTimePeriod(1 minute, producerSettings, topics.head)
        bucketContents <- akka.pattern.after(10 seconds)(
                            S3.listBucket(s3Config.dataBucket, None).withAttributes(s3Attrs).runWith(Sink.seq)
                          )
        keysSorted = bucketContents.map(_.key).sortBy(Utils.keyToOffsetDateTime)
        downloaded <-
          Future
            .sequence(keysSorted.map(key => downloadObject(s3Config.dataBucket, key)))
            .map(_.flatten)
      } yield downloaded.flatten.collect { case Some(reducedConsumerRecord) =>
        reducedConsumerRecord
      }

      calculatedFuture.onComplete { _ =>
        cleanTopics(topics)
        backupClientWrapped.shutdown()
      }
      val downloaded = calculatedFuture.futureValue

      // Only care about ordering when it comes to key
      val downloadedGroupedAsKey = downloaded
        .groupBy(_.key)
        .view
        .mapValues { reducedConsumerRecords =>
          reducedConsumerRecords.map(_.value)
        }
        .toMap

      val inputAsKey = data
        .groupBy(_.key)
        .view
        .mapValues { reducedConsumerRecords =>
          reducedConsumerRecords.map(_.value)
        }
        .toMap

      downloadedGroupedAsKey mustMatchTo inputAsKey
    }
  }

  property(
    "Concurrent backups using real Kafka cluster with a single key",
    RealS3Available
  ) {
    forAll(
      kafkaDataWithMinByteSizeGen(S3.MinChunkSize, 2, reducedConsumerRecordsToJson),
      s3ConfigGen(useVirtualDotHost, bucketPrefix),
      s3ConfigGen(useVirtualDotHost, bucketPrefix),
      kafkaConsumerGroupGen,
      kafkaConsumerGroupGen
    ) {
      (kafkaDataInChunksWithTimePeriod: KafkaDataInChunksWithTimePeriod,
       firstS3Config: S3Config,
       secondS3Config: S3Config,
       firstKafkaConsumerGroup: String,
       secondKafkaConsumerGroup: String
      ) =>
        whenever(
          firstS3Config.dataBucket != secondS3Config.dataBucket && firstKafkaConsumerGroup != secondKafkaConsumerGroup
        ) {
          logger.info(s"Data bucket are ${firstS3Config.dataBucket} and ${secondS3Config.dataBucket}")

          val data = kafkaDataInChunksWithTimePeriod.data.flatten

          val topics = data.map(_.topic).toSet

          val asProducerRecords = toProducerRecords(data)
          val baseSource        = toSource(asProducerRecords, 30 seconds)

          implicit val kafkaClusterConfig: KafkaCluster = KafkaCluster(topics)

          val producerSettings = createProducer()

          val backupClientOneWrapped = {
            implicit val backupConfig: Backup =
              Backup(firstKafkaConsumerGroup, PeriodFromFirst(1 minute), 10 seconds, compression)

            new BackupClientControlWrapper(
              new BackupClient(Some(s3Settings))(
                new KafkaClient(configureConsumer = baseKafkaConfig),
                implicitly,
                implicitly,
                firstS3Config,
                implicitly
              )
            )
          }

          val backupClientTwoWrapped = {
            implicit val backupConfig: Backup =
              Backup(secondKafkaConsumerGroup, PeriodFromFirst(1 minute), 10 seconds, compression)

            new BackupClientControlWrapper(
              new BackupClient(Some(s3Settings))(
                new KafkaClient(configureConsumer = baseKafkaConfig),
                implicitly,
                implicitly,
                secondS3Config,
                implicitly
              )
            )
          }

          val calculatedFuture = for {
            _ <- createTopics(topics)
            _ <- createBucket(firstS3Config.dataBucket)
            _ <- createBucket(secondS3Config.dataBucket)
            _ = backupClientOneWrapped.run()
            _ = backupClientTwoWrapped.run()
            _ <- akka.pattern.after(KafkaInitializationTimeoutConstant)(
                   baseSource
                     .runWith(Producer.plainSink(producerSettings))
                 )
            _      <- sendTopicAfterTimePeriod(1 minute, producerSettings, topics.head)
            keyOne <- getKeyFromSingleDownload(firstS3Config.dataBucket)
            keyTwo <- getKeyFromSingleDownload(secondS3Config.dataBucket)
            downloadedOneFuture = downloadObject(firstS3Config.dataBucket, keyOne)
            downloadedTwoFuture = downloadObject(secondS3Config.dataBucket, keyTwo)
            downloadedOne <- downloadedOneFuture
            downloadedTwo <- downloadedTwoFuture
          } yield (downloadedOne.toList.flatten.collect { case Some(reducedConsumerRecord) =>
                     reducedConsumerRecord
                   },
                   downloadedTwo.toList.flatten.collect { case Some(reducedConsumerRecord) =>
                     reducedConsumerRecord
                   }
          )

          calculatedFuture.onComplete { _ =>
            cleanTopics(topics)
            backupClientOneWrapped.shutdown()
            backupClientTwoWrapped.shutdown()
          }
          val (downloadedOne, downloadedTwo) = calculatedFuture.futureValue

          val downloadedOneGroupedAsKey = downloadedOne
            .groupBy(_.key)
            .view
            .mapValues { reducedConsumerRecords =>
              reducedConsumerRecords.map(_.value)
            }
            .toMap

          val downloadedTwoGroupedAsKey = downloadedTwo
            .groupBy(_.key)
            .view
            .mapValues { reducedConsumerRecords =>
              reducedConsumerRecords.map(_.value)
            }
            .toMap

          val inputAsKey = data
            .groupBy(_.key)
            .view
            .mapValues { reducedConsumerRecords =>
              reducedConsumerRecords.map(_.value)
            }
            .toMap

          downloadedOneGroupedAsKey mustMatchTo inputAsKey
          downloadedTwoGroupedAsKey mustMatchTo inputAsKey
        }
    }
  }

  property(
    "Concurrent backups using real Kafka cluster with a multiple keys",
    RealS3Available
  ) {
    forAll(
      kafkaDataWithTimePeriodsGen(min = 30000, max = 30000),
      s3ConfigGen(useVirtualDotHost, bucketPrefix),
      s3ConfigGen(useVirtualDotHost, bucketPrefix),
      kafkaConsumerGroupGen,
      kafkaConsumerGroupGen
    ) {
      (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod,
       firstS3Config: S3Config,
       secondS3Config: S3Config,
       firstKafkaConsumerGroup: String,
       secondKafkaConsumerGroup: String
      ) =>
        whenever(
          firstS3Config.dataBucket != secondS3Config.dataBucket && firstKafkaConsumerGroup != secondKafkaConsumerGroup
        ) {
          logger.info(s"Data bucket are ${firstS3Config.dataBucket} and ${secondS3Config.dataBucket}")

          val data = kafkaDataWithTimePeriod.data

          val topics = data.map(_.topic).toSet

          val asProducerRecords = toProducerRecords(data)
          val baseSource        = toSource(asProducerRecords, 30 seconds)

          implicit val kafkaClusterConfig: KafkaCluster = KafkaCluster(topics)

          val producerSettings = createProducer()

          val backupClientOneWrapped = {
            implicit val backupConfig: Backup =
              Backup(firstKafkaConsumerGroup, PeriodFromFirst(1 second), 10 seconds, compression)

            new BackupClientControlWrapper(
              new BackupClient(Some(s3Settings))(
                new KafkaClient(configureConsumer = baseKafkaConfig),
                implicitly,
                implicitly,
                firstS3Config,
                implicitly
              )
            )
          }

          val backupClientTwoWrapped = {
            implicit val backupConfig: Backup =
              Backup(secondKafkaConsumerGroup, PeriodFromFirst(1 second), 10 seconds, compression)

            new BackupClientControlWrapper(
              new BackupClient(Some(s3Settings))(
                new KafkaClient(configureConsumer = baseKafkaConfig),
                implicitly,
                implicitly,
                secondS3Config,
                implicitly
              )
            )
          }

          val calculatedFuture = for {
            _ <- createTopics(topics)
            _ <- createBucket(firstS3Config.dataBucket)
            _ <- createBucket(secondS3Config.dataBucket)
            _ = backupClientOneWrapped.run()
            _ = backupClientTwoWrapped.run()
            _ <- akka.pattern.after(KafkaInitializationTimeoutConstant)(
                   baseSource
                     .runWith(Producer.plainSink(producerSettings))
                 )

            _ <- sendTopicAfterTimePeriod(1 minute, producerSettings, topics.head)
            (bucketContentsOne, bucketContentsTwo) <-
              akka.pattern.after(10 seconds)(for {
                bucketContentsOne <-
                  S3.listBucket(firstS3Config.dataBucket, None).withAttributes(s3Attrs).runWith(Sink.seq)
                bucketContentsTwo <-
                  S3.listBucket(secondS3Config.dataBucket, None).withAttributes(s3Attrs).runWith(Sink.seq)

              } yield (bucketContentsOne, bucketContentsTwo))
            keysSortedOne = bucketContentsOne.map(_.key).sortBy(Utils.keyToOffsetDateTime)
            keysSortedTwo = bucketContentsTwo.map(_.key).sortBy(Utils.keyToOffsetDateTime)
            downloadedOneFuture =
              Future
                .sequence(keysSortedOne.map(key => downloadObject(firstS3Config.dataBucket, key)))
                .map(_.flatten)

            downloadedTwoFuture =
              Future
                .sequence(keysSortedTwo.map(key => downloadObject(secondS3Config.dataBucket, key)))
                .map(_.flatten)
            downloadedOne <- downloadedOneFuture
            downloadedTwo <- downloadedTwoFuture
          } yield (downloadedOne.flatten.collect { case Some(reducedConsumerRecord) =>
                     reducedConsumerRecord
                   },
                   downloadedTwo.flatten.collect { case Some(reducedConsumerRecord) =>
                     reducedConsumerRecord
                   }
          )

          calculatedFuture.onComplete { _ =>
            cleanTopics(topics)
            backupClientOneWrapped.shutdown()
            backupClientTwoWrapped.shutdown()
          }
          val (downloadedOne, downloadedTwo) = calculatedFuture.futureValue

          // Only care about ordering when it comes to key
          val downloadedGroupedAsKeyOne = downloadedOne
            .groupBy(_.key)
            .view
            .mapValues { reducedConsumerRecords =>
              reducedConsumerRecords.map(_.value)
            }
            .toMap

          val downloadedGroupedAsKeyTwo = downloadedTwo
            .groupBy(_.key)
            .view
            .mapValues { reducedConsumerRecords =>
              reducedConsumerRecords.map(_.value)
            }
            .toMap

          val inputAsKey = data
            .groupBy(_.key)
            .view
            .mapValues { reducedConsumerRecords =>
              reducedConsumerRecords.map(_.value)
            }
            .toMap

          downloadedGroupedAsKeyOne mustMatchTo inputAsKey
          downloadedGroupedAsKeyTwo mustMatchTo inputAsKey
        }
    }
  }

}
