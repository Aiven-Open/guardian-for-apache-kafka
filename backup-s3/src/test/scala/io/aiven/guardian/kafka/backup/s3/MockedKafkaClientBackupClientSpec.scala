package io.aiven.guardian.kafka.backup.s3

import akka.actor.ActorSystem
import akka.stream.alpakka.s3.S3Settings
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.softwaremill.diffx.scalatest.DiffMustMatcher._
import io.aiven.guardian.akka.AnyPropTestKit
import io.aiven.guardian.kafka.Generators._
import io.aiven.guardian.kafka.Utils
import io.aiven.guardian.kafka.backup.MockedBackupClientInterface
import io.aiven.guardian.kafka.backup.MockedKafkaClientInterface
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.backup.configs.PeriodFromFirst
import io.aiven.guardian.kafka.codecs.Circe._
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.kafka.s3.Generators._
import io.aiven.guardian.kafka.s3.S3Spec
import io.aiven.guardian.kafka.s3.configs.{S3 => S3Config}
import org.mdedetrich.akka.stream.support.CirceStreamSupport
import org.scalatest.matchers.must.Matchers

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.language.postfixOps

class MockedKafkaClientBackupClientSpec
    extends AnyPropTestKit(ActorSystem("MockedKafkaClientBackupClientSpec"))
    with S3Spec
    with Matchers {
  override lazy val s3Settings: S3Settings = S3Settings()

  /** Virtual Dot Host in bucket names are disabled because you need an actual DNS certificate otherwise AWS will fail
    * on bucket creation
    */
  override lazy val useVirtualDotHost: Boolean            = false
  override lazy val bucketPrefix: Option[String]          = Some("guardian-")
  override lazy val enableCleanup: Option[FiniteDuration] = Some(5 seconds)

  property(
    "Creating many objects in a small period of time works despite S3's in progress multipart upload eventual consistency issues",
    RealS3Available
  ) {
    forAll(kafkaDataWithTimePeriodsGen(100, 100, 1000, tailingSentinelValue = true),
           s3ConfigGen(useVirtualDotHost, bucketPrefix)
    ) { (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod, s3Config: S3Config) =>
      logger.info(s"Data bucket is ${s3Config.dataBucket}")
      val data = kafkaDataWithTimePeriod.data

      implicit val config: S3Config = s3Config
      implicit val backupConfig: Backup =
        Backup(MockedBackupClientInterface.KafkaGroupId, PeriodFromFirst(1 second), 10 seconds)

      val backupClient =
        new BackupClient(Some(s3Settings))(new MockedKafkaClientInterface(Source(data)),
                                           implicitly,
                                           implicitly,
                                           implicitly,
                                           implicitly
        )

      val calculatedFuture = for {
        _ <- createBucket(s3Config.dataBucket)
        _ = backupClient.backup.run()
        bucketContents <- akka.pattern.after(10 seconds)(
                            S3.listBucket(s3Config.dataBucket, None).withAttributes(s3Attrs).runWith(Sink.seq)
                          )
        keysSorted = bucketContents.map(_.key).sortBy(Utils.keyToOffsetDateTime)
        downloaded <-
          Future
            .sequence(keysSorted.map { key =>
              S3.download(s3Config.dataBucket, key)
                .withAttributes(s3Attrs)
                .runWith(Sink.head)
                .flatMap {
                  case Some((downloadSource, _)) =>
                    downloadSource
                      .via(CirceStreamSupport.decode[List[Option[ReducedConsumerRecord]]])
                      .runWith(Sink.seq)
                  case None => throw new Exception(s"Expected object in bucket ${s3Config.dataBucket} with key $key")
                }

            })
            .map(_.flatten)

      } yield downloaded.flatten.collect { case Some(reducedConsumerRecord) =>
        reducedConsumerRecord
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
        .dropRight(1) // Drop the generated sentinel value which we don't care about
        .groupBy(_.key)
        .view
        .mapValues { reducedConsumerRecords =>
          reducedConsumerRecords.map(_.value)
        }
        .toMap

      downloadedGroupedAsKey mustMatchTo inputAsKey
    }
  }

}
