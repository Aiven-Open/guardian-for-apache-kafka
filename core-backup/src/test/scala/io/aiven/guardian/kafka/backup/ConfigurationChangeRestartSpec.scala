package io.aiven.guardian.kafka.backup

import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx.scalatest.DiffMustMatcher._
import io.aiven.guardian.kafka.Generators.KafkaDataWithTimePeriod
import io.aiven.guardian.kafka.Generators.kafkaDataWithTimePeriodsGen
import io.aiven.guardian.kafka.TestUtils.waitForStartOfTimeUnit
import io.aiven.guardian.kafka.backup.configs.ChronoUnitSlice
import io.aiven.guardian.kafka.backup.configs.Compression
import io.aiven.guardian.kafka.codecs.Circe._
import io.aiven.guardian.kafka.models.BackupObjectMetadata
import io.aiven.guardian.kafka.models.Gzip
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.pekko.AnyPropTestKit
import io.aiven.guardian.pekko.PekkoStreamTestKit
import org.apache.pekko
import org.mdedetrich.pekko.stream.support.CirceStreamSupport
import org.scalatest.Inspectors
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.typelevel.jawn.AsyncParser

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentLinkedQueue

import pekko.actor.ActorSystem
import pekko.stream.scaladsl.Keep
import pekko.stream.scaladsl.Sink
import pekko.stream.scaladsl.Source
import pekko.util.ByteString

class ConfigurationChangeRestartSpec
    extends AnyPropTestKit(ActorSystem("ConfigurationChangeSpec"))
    with PekkoStreamTestKit
    with Matchers
    with ScalaFutures
    with ScalaCheckPropertyChecks {

  implicit val ec: ExecutionContext            = system.dispatcher
  implicit val defaultPatience: PatienceConfig = PatienceConfig(90 seconds, 100 millis)

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1)

  property("GZip compression enabled initially and then BackupClient restarted with compression disabled") { _ =>
    implicit val generatorDrivenConfig: PropertyCheckConfiguration =
      PropertyCheckConfiguration(minSuccessful = 1)

    forAll(kafkaDataWithTimePeriodsGen()) { (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod) =>
      val commitStorage = new ConcurrentLinkedDeque[Long]()
      val backupStorage = new ConcurrentLinkedQueue[(String, ByteString)]()
      val data          = kafkaDataWithTimePeriod.data

      val mockOne = new MockedBackupClientInterfaceWithMockedKafkaData(
        Source(data),
        ChronoUnitSlice(ChronoUnit.MINUTES),
        Some(Compression(Gzip, None)),
        commitStorage,
        backupStorage,
        stopAfterDuration = Some(kafkaDataWithTimePeriod.periodSlice),
        handleOffsets = true
      )

      val mockTwo = new MockedBackupClientInterfaceWithMockedKafkaData(Source(data),
                                                                       ChronoUnitSlice(ChronoUnit.MINUTES),
                                                                       None,
                                                                       commitStorage,
                                                                       backupStorage,
                                                                       stopAfterDuration = None,
                                                                       handleOffsets = true
      )

      val calculatedFuture = for {
        _ <- waitForStartOfTimeUnit(ChronoUnit.MINUTES)
        _ <- mockOne.backup.run()
        keysWithGzip <- pekko.pattern.after(PekkoStreamInitializationConstant)(
                          Future.successful(
                            backupStorage.asScala.map { case (key, _) => key }.toSet
                          )
                        )
        _ <- mockTwo.backup.run()
        keysWithoutGzip <-
          pekko.pattern.after(PekkoStreamInitializationConstant)(Future.successful {
            val allKeys = backupStorage.asScala.map { case (key, _) => key }.toSet
            allKeys diff keysWithGzip
          })
        processedRecords = mockTwo.mergeBackedUpData(compression = Some(Gzip))
        asRecords <- Future.sequence(processedRecords.map { case (key, byteString) =>
                       Source
                         .single(byteString)
                         .via(CirceStreamSupport.decode[Option[ReducedConsumerRecord]](AsyncParser.UnwrapArray))
                         .collect { case Some(value) =>
                           value
                         }
                         .toMat(Sink.collection)(Keep.right)
                         .run()
                         .map(records => (key, records))
                     })
      } yield (asRecords, keysWithGzip, keysWithoutGzip)

      val (records, keysWithGzip, keysWithoutGzip) = calculatedFuture.futureValue

      val observed = records.flatMap { case (_, values) => values }

      Inspectors.forEvery(keysWithGzip)(key => BackupObjectMetadata.fromKey(key).compression must contain(Gzip))
      Inspectors.forEvery(keysWithoutGzip)(key => BackupObjectMetadata.fromKey(key).compression mustBe empty)
      data mustMatchTo observed
    }
  }

  property("no compression enabled initially and then BackupClient restarted with GZip compression enabled") { _ =>
    forAll(kafkaDataWithTimePeriodsGen()) { (kafkaDataWithTimePeriod: KafkaDataWithTimePeriod) =>
      val commitStorage = new ConcurrentLinkedDeque[Long]()
      val backupStorage = new ConcurrentLinkedQueue[(String, ByteString)]()
      val data          = kafkaDataWithTimePeriod.data

      val mockOne = new MockedBackupClientInterfaceWithMockedKafkaData(
        Source(data),
        ChronoUnitSlice(ChronoUnit.MINUTES),
        None,
        commitStorage,
        backupStorage,
        stopAfterDuration = Some(kafkaDataWithTimePeriod.periodSlice),
        handleOffsets = true
      )

      val mockTwo = new MockedBackupClientInterfaceWithMockedKafkaData(Source(data),
                                                                       ChronoUnitSlice(ChronoUnit.MINUTES),
                                                                       Some(Compression(Gzip, None)),
                                                                       commitStorage,
                                                                       backupStorage,
                                                                       stopAfterDuration = None,
                                                                       handleOffsets = true
      )

      val calculatedFuture = for {
        _ <- waitForStartOfTimeUnit(ChronoUnit.MINUTES)
        _ <- mockOne.backup.run()
        keysWithoutGzip <- pekko.pattern.after(PekkoStreamInitializationConstant)(
                             Future.successful(
                               backupStorage.asScala.map { case (key, _) => key }.toSet
                             )
                           )
        _ <- mockTwo.backup.run()
        keysWithGzip <-
          pekko.pattern.after(PekkoStreamInitializationConstant)(Future.successful {
            val allKeys = backupStorage.asScala.map { case (key, _) => key }.toSet
            allKeys diff keysWithoutGzip
          })
        processedRecords = mockTwo.mergeBackedUpData(compression = Some(Gzip))
        asRecords <- Future.sequence(processedRecords.map { case (key, byteString) =>
                       Source
                         .single(byteString)
                         .via(CirceStreamSupport.decode[Option[ReducedConsumerRecord]](AsyncParser.UnwrapArray))
                         .collect { case Some(value) =>
                           value
                         }
                         .toMat(Sink.collection)(Keep.right)
                         .run()
                         .map(records => (key, records))
                     })
      } yield (asRecords, keysWithGzip, keysWithoutGzip)

      val (records, keysWithGzip, keysWithoutGzip) = calculatedFuture.futureValue

      val observed = records.flatMap { case (_, values) => values }

      Inspectors.forEvery(keysWithGzip)(key => BackupObjectMetadata.fromKey(key).compression must contain(Gzip))
      Inspectors.forEvery(keysWithoutGzip)(key => BackupObjectMetadata.fromKey(key).compression mustBe empty)
      data mustMatchTo observed
    }
  }
}
