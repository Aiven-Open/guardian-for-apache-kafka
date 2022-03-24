package io.aiven.guardian.kafka.restore

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Attributes
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging
import io.aiven.guardian.kafka.ExtensionsMethods._
import io.aiven.guardian.kafka.Utils
import io.aiven.guardian.kafka.codecs.Circe._
import io.aiven.guardian.kafka.configs.KafkaCluster
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.kafka.restore.configs.Restore
import org.mdedetrich.akka.stream.support.CirceStreamSupport
import org.typelevel.jawn.AsyncParser

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import java.time.OffsetDateTime

trait RestoreClientInterface[T <: KafkaProducerInterface] extends StrictLogging {
  implicit val kafkaProducerInterface: T
  implicit val restoreConfig: Restore
  implicit val kafkaClusterConfig: KafkaCluster
  implicit val system: ActorSystem
  val maybeKillSwitch: Option[SharedKillSwitch]
  val maybeAttributes: Option[Attributes] = None

  def retrieveBackupKeys: Future[List[String]]

  def downloadFlow: Flow[String, ByteString, NotUsed]

  private[kafka] def keysWithOffsetDateTime(keys: List[String]): List[(String, OffsetDateTime)] = keys.map { key =>
    (key, Utils.keyToOffsetDateTime(key))
  }

  private[kafka] def finalKeys: Future[List[String]] = {
    implicit val ec: ExecutionContext = system.dispatcher

    for {
      backupKeys <- retrieveBackupKeys
      withTime = keysWithOffsetDateTime(backupKeys)
      sorted = withTime.sortBy { case (_, time) =>
                 time
               }

      latest = restoreConfig.fromWhen match {
                 case Some(pickedDate) =>
                   val index = sorted.indexWhere { case (_, time) =>
                     time >= pickedDate
                   }
                   index match {
                     case 0 => sorted
                     case -1 =>
                       sorted.lastOption match {
                         case Some((key, value)) =>
                           // Its still technically possible that the last key can contain a picked value.
                           List((key, value))
                         case _ => List.empty
                       }
                     case index =>
                       val (_, rest) = sorted.splitAt(index - 1)
                       rest
                   }
                 case None => sorted
               }
    } yield latest.map { case (key, _) => key }
  }

  private[kafka] def checkTopicInConfig(reducedConsumerRecord: ReducedConsumerRecord): Boolean =
    kafkaClusterConfig.topics.contains(reducedConsumerRecord.topic)

  private[kafka] def checkTopicGreaterThanTime(reducedConsumerRecord: ReducedConsumerRecord): Boolean =
    restoreConfig.fromWhen match {
      case Some(pickedDate) =>
        reducedConsumerRecord.toOffsetDateTime >= pickedDate
      case None => true
    }

  private[kafka] def restoreKey(key: String): Future[Done] = {
    val base = Source
      .single(key)
      .via(downloadFlow)
      .via(CirceStreamSupport.decode[Option[ReducedConsumerRecord]](AsyncParser.UnwrapArray))
      .collect {
        case Some(reducedConsumerRecord)
            if checkTopicInConfig(reducedConsumerRecord) && checkTopicGreaterThanTime(reducedConsumerRecord) =>
          reducedConsumerRecord
      }

    maybeKillSwitch
      .fold(base)(killSwitch => base.via(killSwitch.flow))
      .runWith(kafkaProducerInterface.getSink)
  }

  def restore: Future[Done] = {
    implicit val ec: ExecutionContext = system.dispatcher

    for {
      keys <- finalKeys
      _    <- Utils.runSequentially(keys.map(key => () => restoreKey(key)))
    } yield Done
  }

}
