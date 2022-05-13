package io.aiven.guardian.kafka.restore

import akka.kafka.ProducerSettings
import akka.stream.KillSwitches
import akka.stream.SharedKillSwitch
import cats.data.ValidatedNel
import com.monovore.decline._
import com.monovore.decline.time._
import com.typesafe.scalalogging.Logger
import io.aiven.guardian.cli.MainUtils
import io.aiven.guardian.cli.arguments.StorageOpt
import io.aiven.guardian.cli.options.Options
import io.aiven.guardian.kafka.configs.KafkaCluster
import io.aiven.guardian.kafka.restore.configs.Restore
import io.aiven.guardian.kafka.s3.configs.S3
import org.apache.kafka.clients.producer.ProducerConfig
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

import java.time.OffsetDateTime
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference

class Entry(val initializedApp: AtomicReference[Option[App]] = new AtomicReference(None))
    extends CommandApp(
      name = "guardian-restore",
      header = "Guardian cli Backup Tool",
      main = {
        val logClassName: String = getClass.getName

        // This is imported here because otherwise the reference to getClass above is ambiguous
        import io.aiven.guardian.cli.arguments.PropertiesOpt._
        import cats.implicits._

        val fromWhenOpt: Opts[Option[OffsetDateTime]] =
          Opts.option[OffsetDateTime]("from-when", help = "Only restore topics from a given time").orNone

        val topicNameMapArgument: Opts[Option[Map[String, String]]] = {

          val colonTupleArgument = new Argument[(String, String)] {
            override def read(string: String): ValidatedNel[String, (String, String)] =
              string.split(":") match {
                case Array(left, right) => (left, right).validNel
                case _                  => "Expected a colon delimited key:value".invalidNel
              }

            override def defaultMetavar: String = "key:value"
          }

          Opts
            .options[(String, String)]("override-topics", help = "Restore a topic under a different name")(
              colonTupleArgument
            )
            .map(_.toList.toMap)
            .orNone

        }

        val restoreOpt = (fromWhenOpt, topicNameMapArgument).tupled.map { case (fromWhen, overrideTopics) =>
          Restore(fromWhen, overrideTopics)
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

        val initialKafkaProducerSettingsOpt
            : Opts[Option[ProducerSettings[Array[Byte], Array[Byte]] => ProducerSettings[Array[Byte], Array[Byte]]]] =
          Options.bootstrapServersOpt.mapValidated {
            case Some(value) =>
              val block =
                (block: ProducerSettings[Array[Byte], Array[Byte]]) =>
                  block.withBootstrapServers(value.toList.mkString(","))

              Some(block).validNel
            case None
                if Options.checkConfigKeyIsDefined("akka.kafka.producer.kafka-clients.bootstrap.servers") || Options
                  .checkConfigKeyIsDefined("kafka-client.bootstrap.servers") =>
              None.validNel
            case _ => "bootstrap-servers is a mandatory value that needs to be configured".invalidNel
          }

        val singleMessagePerRequestOpt =
          Opts
            .flag(
              "single-message-per-kafka-request",
              "A set of Kafka producer configuration options that only sends one message per request to provide exactly once message semantics without using Kafka transactions."
            )
            .orFalse

        val producerPropertiesOpt = Opts
          .option[Properties]("producer-properties",
                              "Path to a java .properties file to be passed to the underlying KafkaClients producer"
          )
          .orNone

        val propertiesConsumerSettingsOpt = producerPropertiesOpt.mapValidated {
          case Some(value) =>
            val block =
              (block: ProducerSettings[Array[Byte], Array[Byte]]) => block.withProperties(value.asScala.toMap)

            Some(block).validNel
          case None =>
            None.validNel
        }

        val bootstrapConsumerSettingsOpt
            : Opts[Option[ProducerSettings[Array[Byte], Array[Byte]] => ProducerSettings[Array[Byte], Array[Byte]]]] =
          (initialKafkaProducerSettingsOpt, singleMessagePerRequestOpt).tupled.map {
            case (producerSettings, true) =>
              val applySettings = (block: ProducerSettings[Array[Byte], Array[Byte]]) =>
                block
                  .withProperties(
                    Map(
                      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG             -> true.toString,
                      ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> 1.toString,
                      ProducerConfig.BATCH_SIZE_CONFIG                     -> 0.toString
                    )
                  )
                  .withParallelism(1)

              producerSettings.map(_ andThen applySettings).orElse(Some(applySettings))
            case (producerSettings, false) => producerSettings
          }

        (Options.logbackFileOpt,
         Options.storageOpt,
         Options.kafkaClusterOpt,
         propertiesConsumerSettingsOpt,
         bootstrapConsumerSettingsOpt,
         s3Opt,
         restoreOpt
        ).mapN {
          (logbackFile, storage, kafkaCluster, propertiesConsumerSettings, bootstrapConsumerSettings, s3, restore) =>
            logbackFile.foreach(path => MainUtils.setLogbackFile(path, LoggerFactory.getILoggerFactory))
            lazy val logger: Logger =
              Logger(LoggerFactory.getLogger(logClassName))
            val killSwitch = KillSwitches.shared("restore-kill-switch")
            val app = storage match {
              case StorageOpt.S3 =>
                new S3App {
                  override lazy val kafkaClusterConfig: KafkaCluster          = kafkaCluster
                  override lazy val s3Config: S3                              = s3
                  override lazy val restoreConfig: Restore                    = restore
                  override lazy val maybeKillSwitch: Option[SharedKillSwitch] = Some(killSwitch)
                  override lazy val kafkaProducer: KafkaProducer = {
                    val finalProducerSettings =
                      (propertiesConsumerSettings.toList ++ bootstrapConsumerSettings.toList).reduceLeft(_ andThen _)

                    new KafkaProducer(Some(finalProducerSettings))(actorSystem, restoreConfig)
                  }
                }
            }
            initializedApp.set(Some(app))
            Runtime.getRuntime.addShutdownHook(new Thread {
              logger.info("Shutdown of Guardian detected")
              killSwitch.shutdown()
              Await.result(app.actorSystem.terminate(), 5 minutes)
            })
            Await.result(app.run(), Duration.Inf)
        }
      }
    )

object Main extends Entry
