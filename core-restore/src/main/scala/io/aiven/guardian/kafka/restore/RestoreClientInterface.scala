package io.aiven.guardian.kafka.restore

import com.typesafe.scalalogging.LazyLogging
import io.aiven.guardian.kafka.ExtensionsMethods._
import io.aiven.guardian.kafka.Utils
import io.aiven.guardian.kafka.codecs.Circe._
import io.aiven.guardian.kafka.configs.KafkaCluster
import io.aiven.guardian.kafka.models.BackupObjectMetadata
import io.aiven.guardian.kafka.models.Gzip
import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.kafka.restore.configs.Restore
import org.apache.pekko
import org.mdedetrich.pekko.stream.support.CirceStreamSupport
import org.typelevel.jawn.AsyncParser

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import java.time.OffsetDateTime

import pekko.Done
import pekko.NotUsed
import pekko.actor.ActorSystem
import pekko.stream.Attributes
import pekko.stream.KillSwitches
import pekko.stream.UniqueKillSwitch
import pekko.stream.scaladsl.Compression
import pekko.stream.scaladsl.Concat
import pekko.stream.scaladsl.Flow
import pekko.stream.scaladsl.Keep
import pekko.stream.scaladsl.RunnableGraph
import pekko.stream.scaladsl.Source
import pekko.util.ByteString

trait RestoreClientInterface[T <: KafkaProducerInterface] extends LazyLogging {
  implicit val kafkaProducerInterface: T
  implicit val restoreConfig: Restore
  implicit val kafkaClusterConfig: KafkaCluster
  implicit val system: ActorSystem
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

  private[kafka] def restoreKey(key: String): Source[ByteString, NotUsed] = {
    val source = Source
      .single(key)
      .via(downloadFlow)

    BackupObjectMetadata.fromKey(key).compression match {
      case Some(Gzip) => source.via(Compression.gunzip())
      case None       => source
    }
  }

  def restore: RunnableGraph[(UniqueKillSwitch, Future[Done])] = {
    val sourceWithCompression = Source.future(finalKeys).flatMapConcat { keys =>
      keys.map(key => restoreKey(key)) match {
        case first :: Nil            => first
        case first :: second :: Nil  => Source.combine(first, second)(Concat(_))
        case first :: second :: rest => Source.combine(first, second, rest: _*)(Concat(_))
        case Nil                     => Source.empty[ByteString]
      }
    }

    val asReducedConsumerRecord = sourceWithCompression
      .via(CirceStreamSupport.decode[Option[ReducedConsumerRecord]](AsyncParser.UnwrapArray, multiValue = true))
      .collect {
        case Some(reducedConsumerRecord)
            if checkTopicInConfig(reducedConsumerRecord) && checkTopicGreaterThanTime(reducedConsumerRecord) =>
          reducedConsumerRecord
      }

    asReducedConsumerRecord.viaMat(KillSwitches.single)(Keep.right).toMat(kafkaProducerInterface.getSink)(Keep.both)
  }

}
