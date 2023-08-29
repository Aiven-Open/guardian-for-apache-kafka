package io.aiven.guardian.kafka.restore.s3

import com.softwaremill.diffx.scalatest.DiffMustMatcher._
import io.aiven.guardian.kafka.Generators._
import io.aiven.guardian.kafka.KafkaClusterTest
import io.aiven.guardian.kafka.TestUtils._
import io.aiven.guardian.kafka.backup.BackupClientControlWrapper
import io.aiven.guardian.kafka.backup.KafkaConsumer
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.backup.configs.Compression
import io.aiven.guardian.kafka.backup.configs.PeriodFromFirst
import io.aiven.guardian.kafka.backup.s3.BackupClient
import io.aiven.guardian.kafka.configs.{KafkaCluster => KafkaClusterConfig}
import io.aiven.guardian.kafka.restore.KafkaProducer
import io.aiven.guardian.kafka.restore.configs.{Restore => RestoreConfig}
import io.aiven.guardian.kafka.s3.Generators.s3ConfigGen
import io.aiven.guardian.kafka.s3.S3Spec
import io.aiven.guardian.kafka.s3.configs.{S3 => S3Config}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.pekko
import org.scalatest.TestData
import org.scalatest.matchers.must.Matchers
import org.scalatest.propspec.FixtureAnyPropSpecLike
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.language.postfixOps

import pekko.kafka.ConsumerSettings
import pekko.kafka.Subscriptions
import pekko.kafka.scaladsl.Consumer
import pekko.kafka.scaladsl.Consumer.DrainingControl
import pekko.kafka.scaladsl.Producer
import pekko.stream.connectors.s3.S3Settings
import pekko.stream.connectors.s3.scaladsl.S3
import pekko.stream.scaladsl.Sink

trait RealS3RestoreClientTest
    extends FixtureAnyPropSpecLike
    with S3Spec
    with Matchers
    with KafkaClusterTest
    with ScalaCheckPropertyChecks {

  def compression: Option[Compression]

  override lazy val s3Settings: S3Settings = S3Settings()

  def waitForSingleDownload(dataBucket: String): Future[Unit] = waitForS3Download(
    dataBucket,
    {
      case Seq(_) => ()
      case rest =>
        throw DownloadNotReady(rest)
    }
  )

  /** Virtual Dot Host in bucket names are disabled because you need an actual DNS certificate otherwise AWS will fail
    * on bucket creation
    */
  override lazy val useVirtualDotHost: Boolean            = false
  override lazy val bucketPrefix: Option[String]          = Some("guardian-")
  override lazy val enableCleanup: Option[FiniteDuration] = Some(5 seconds)

  property("Round-trip with backup and restore") { implicit td: TestData =>
    forAll(
      kafkaDataWithMinSizeRenamedTopicsGen(S3.MinChunkSize, 2, reducedConsumerRecordsToJson),
      s3ConfigGen(useVirtualDotHost, bucketPrefix),
      kafkaConsumerGroupGen
    ) {
      (kafkaDataInChunksWithTimePeriodRenamedTopics: KafkaDataInChunksWithTimePeriodRenamedTopics,
       s3Config: S3Config,
       kafkaConsumerGroup: String
      ) =>
        implicit val restoreConfig: RestoreConfig =
          RestoreConfig(None, Some(kafkaDataInChunksWithTimePeriodRenamedTopics.renamedTopics))
        implicit val kafkaProducer: KafkaProducer = new KafkaProducer(configureProducer = baseProducerConfig)
        implicit val kafkaClusterConfig: KafkaClusterConfig =
          KafkaClusterConfig(kafkaDataInChunksWithTimePeriodRenamedTopics.topics)

        val data = kafkaDataInChunksWithTimePeriodRenamedTopics.data.flatten

        val renamedTopics = kafkaDataInChunksWithTimePeriodRenamedTopics.renamedTopics.values.toSet

        val asProducerRecords = toProducerRecords(data)
        val baseSource        = toSource(asProducerRecords, 30 seconds)

        implicit val config: S3Config = s3Config
        implicit val backupConfig: Backup =
          Backup(kafkaConsumerGroup, PeriodFromFirst(1 minute), 10 seconds, compression)

        val backupClientWrapped =
          new BackupClientControlWrapper(
            new BackupClient(Some(s3Settings))(new KafkaConsumer(configureConsumer = baseKafkaConfig),
                                               implicitly,
                                               implicitly,
                                               implicitly,
                                               implicitly
            )
          )

        val restoreClient =
          new RestoreClient[KafkaProducer](Some(s3Settings))

        val producerSettings = createProducer()

        val calculatedFuture = for {
          _ <- createTopics(kafkaDataInChunksWithTimePeriodRenamedTopics.topics)
          _ <- createBucket(s3Config.dataBucket)
          _ = backupClientWrapped.run()
          _ <- pekko.pattern.after(KafkaInitializationTimeoutConstant)(
                 baseSource
                   .runWith(Producer.plainSink(producerSettings))
               )
          _ <- sendTopicAfterTimePeriod(1 minute,
                                        producerSettings,
                                        kafkaDataInChunksWithTimePeriodRenamedTopics.topics.head
               )
          _ <- waitForSingleDownload(s3Config.dataBucket)
          restoreResultConsumerTopicSettings =
            ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)
              .withBootstrapServers(
                container.bootstrapServers
              )
              .withGroupId(kafkaConsumerGroup)
          restoreResultConsumerSource =
            Consumer
              .plainSource(restoreResultConsumerTopicSettings, Subscriptions.topics(renamedTopics))
          eventualRestoredTopics = restoreResultConsumerSource.toMat(Sink.collection)(DrainingControl.apply).run()
          _ <- createTopics(renamedTopics)
          _ <- pekko.pattern.after(5 seconds) {
                 val (_, future) = restoreClient.restore.run()
                 future
               }
          receivedTopics <- pekko.pattern.after(1 minute)(eventualRestoredTopics.drainAndShutdown())
          asConsumerRecords = receivedTopics.map(KafkaConsumer.consumerRecordToReducedConsumerRecord)
        } yield asConsumerRecords.toList

        calculatedFuture.onCompleteLogError { () =>
          cleanTopics(kafkaDataInChunksWithTimePeriodRenamedTopics.topics)
          cleanTopics(renamedTopics)
          backupClientWrapped.shutdown()
        }
        val restoredConsumerRecords = calculatedFuture.futureValue

        val restoredGroupedAsKey = restoredConsumerRecords
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

        restoredGroupedAsKey mustMatchTo inputAsKey
    }

  }

}
