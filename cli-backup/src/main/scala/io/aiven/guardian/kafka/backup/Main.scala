package io.aiven.guardian.kafka.backup

import akka.kafka.ConsumerSettings
import cats.implicits._
import com.monovore.decline._
import io.aiven.guardian.cli.MainUtils
import io.aiven.guardian.cli.arguments.PropertiesOpt._
import io.aiven.guardian.cli.arguments.StorageOpt
import io.aiven.guardian.cli.options.Options
import io.aiven.guardian.kafka.backup.configs.Backup
import io.aiven.guardian.kafka.backup.configs.ChronoUnitSlice
import io.aiven.guardian.kafka.backup.configs.PeriodFromFirst
import io.aiven.guardian.kafka.backup.configs.TimeConfiguration
import io.aiven.guardian.kafka.configs.KafkaCluster
import io.aiven.guardian.kafka.s3.configs.S3

import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

import java.time.temporal.ChronoUnit
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference

class Entry(val initializedApp: AtomicReference[Option[(App[_], Promise[Unit])]] = new AtomicReference(None))
    extends CommandApp(
      name = "guardian-backup",
      header = "Guardian cli Backup Tool",
      main = {
        val groupIdOpt: Opts[Option[String]] =
          Opts.option[String]("kafka-group-id", help = "Kafka group id for the consumer").orNone

        val periodFromFirstOpt =
          Opts
            .option[FiniteDuration]("period-from-first", help = "Duration for period-from-first configured backup")
            .map(PeriodFromFirst.apply)

        val chronoUnitSliceOpt =
          Opts
            .option[ChronoUnit]("chrono-unit-slice", help = "ChronoUnit for chrono-unit-slice configured backup")
            .map(ChronoUnitSlice.apply)

        val timeConfigurationOpt: Opts[Option[TimeConfiguration]] =
          (periodFromFirstOpt orElse chronoUnitSliceOpt).orNone

        val backupOpt =
          (groupIdOpt, timeConfigurationOpt).tupled.mapValidated { case (maybeGroupId, maybeTimeConfiguration) =>
            import io.aiven.guardian.kafka.backup.Config.backupConfig
            (maybeGroupId, maybeTimeConfiguration) match {
              case (Some(groupId), Some(timeConfiguration)) =>
                Backup(groupId, timeConfiguration).validNel
              case _ =>
                Options
                  .optionalPureConfigValue(() => backupConfig)
                  .toValidNel("Backup config is a mandatory value that needs to be configured")
            }
          }

        val s3Opt = Options.dataBucketOpt.mapValidated { maybeDataBucket =>
          import io.aiven.guardian.kafka.s3.Config
          maybeDataBucket match {
            case Some(value) => S3(dataBucket = value).validNel
            case _ =>
              Options
                .optionalPureConfigValue(() => Config.s3Config)
                .toValidNel("S3 data bucket is a mandatory value that needs to be configured")
          }
        }

        val consumerPropertiesOpt = Opts
          .option[Properties]("consumer-properties",
                              "Path to a java .properties file to be passed to the underlying KafkaClients consumer."
          )
          .orNone

        val propertiesConsumerSettingsOpt = consumerPropertiesOpt.mapValidated {
          case Some(value) =>
            val block =
              (block: ConsumerSettings[Array[Byte], Array[Byte]]) => block.withProperties(value.asScala.toMap)

            Some(block).validNel
          case None =>
            None.validNel
        }

        val bootstrapConsumerSettingsOpt
            : Opts[Option[ConsumerSettings[Array[Byte], Array[Byte]] => ConsumerSettings[Array[Byte], Array[Byte]]]] =
          Options.bootstrapServersOpt.mapValidated {
            case Some(value) =>
              val block =
                (block: ConsumerSettings[Array[Byte], Array[Byte]]) =>
                  block.withBootstrapServers(value.toList.mkString(","))

              Some(block).validNel
            case None if Options.checkConfigKeyIsDefined("akka.kafka.consumer.kafka-clients.bootstrap.servers") =>
              None.validNel
            case _ => "bootstrap-servers is a mandatory value that needs to be configured".invalidNel
          }

        (Options.storageOpt,
         Options.kafkaClusterOpt,
         propertiesConsumerSettingsOpt,
         bootstrapConsumerSettingsOpt,
         s3Opt,
         backupOpt
        ).mapN { (storage, kafkaCluster, propertiesConsumerSettings, bootstrapConsumerSettings, s3, backup) =>
          val app = storage match {
            case StorageOpt.S3 =>
              new S3App {
                override lazy val kafkaClusterConfig: KafkaCluster = kafkaCluster
                override lazy val s3Config: S3                     = s3
                override lazy val backupConfig: Backup             = backup
                override lazy val kafkaClient: KafkaClient = {
                  val finalConsumerSettings =
                    (propertiesConsumerSettings.toList ++ bootstrapConsumerSettings.toList).reduceLeft(_ andThen _)

                  new KafkaClient(Some(finalConsumerSettings))(actorSystem, kafkaClusterConfig, backupConfig)
                }
              }
          }
          val control = app.run()
          val p       = Promise[Unit]()
          initializedApp.set(Some((app, p)))
          Await.result(MainUtils.waitForShutdownSignal(p)(app.executionContext), Duration.Inf)
          Await.result(app.shutdown(control), 5 minutes)
        }
      }
    )

object Main extends Entry()
