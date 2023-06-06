package io.aiven.guardian.kafka.restore

import io.aiven.guardian.kafka.models.ReducedConsumerRecord
import io.aiven.guardian.kafka.restore.configs.Restore
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.pekko

import scala.concurrent.Future

import java.util.Base64

import pekko.Done
import pekko.actor.ActorSystem
import pekko.kafka.ProducerSettings
import pekko.kafka.scaladsl.Producer
import pekko.stream.scaladsl.Sink

class KafkaProducer(
    configureProducer: Option[
      ProducerSettings[Array[Byte], Array[Byte]] => ProducerSettings[Array[Byte], Array[Byte]]
    ] = None
)(implicit system: ActorSystem, restoreConfig: Restore)
    extends KafkaProducerInterface {

  private[kafka] val producerSettings = {
    val base = ProducerSettings(system, new ByteArraySerializer, new ByteArraySerializer)
    configureProducer
      .fold(base)(block => block(base))
  }

  override def getSink: Sink[ReducedConsumerRecord, Future[Done]] =
    Producer.plainSink(producerSettings).contramap[ReducedConsumerRecord] { reducedConsumerRecord =>
      val topic = restoreConfig.overrideTopics match {
        case Some(map) =>
          map.getOrElse(reducedConsumerRecord.topic, reducedConsumerRecord.topic)
        case None => reducedConsumerRecord.topic
      }
      val valueAsByteArray = Base64.getDecoder.decode(reducedConsumerRecord.value)
      reducedConsumerRecord.key match {
        case Some(key) =>
          new ProducerRecord[Array[Byte], Array[Byte]](
            topic,
            Base64.getDecoder.decode(key),
            valueAsByteArray
          )
        case None =>
          new ProducerRecord[Array[Byte], Array[Byte]](
            topic,
            valueAsByteArray
          )
      }
    }
}
