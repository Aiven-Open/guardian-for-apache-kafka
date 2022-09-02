package io.aiven.guardian.kafka

import akka.Done
import akka.kafka.ConsumerSettings
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import com.dimafeng.testcontainers.ForAllTestContainer
import com.dimafeng.testcontainers.KafkaContainer
import io.aiven.guardian.akka.AkkaStreamTestKit
import io.aiven.guardian.kafka.TestUtils.KafkaFutureToCompletableFuture
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.Suite

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.language.postfixOps

trait KafkaClusterTest extends ForAllTestContainer with AkkaStreamTestKit { this: Suite =>

  /** Timeout constant to wait for both Akka Streams plus initialization of consumer/kafka cluster
    */
  val KafkaInitializationTimeoutConstant: FiniteDuration = AkkaStreamInitializationConstant + (2.5 seconds)

  override lazy val container: KafkaContainer = new KafkaContainer()

  def baseKafkaConfig: Some[ConsumerSettings[Array[Byte], Array[Byte]] => ConsumerSettings[Array[Byte], Array[Byte]]] =
    Some(
      _.withBootstrapServers(
        container.bootstrapServers
      )
    )

  /** This config ensures that our producer is atomic since we only ever send a single kafka topic per request and there
    * can only be a single request at a given time
    * @return
    */
  def baseProducerConfig
      : Some[ProducerSettings[Array[Byte], Array[Byte]] => ProducerSettings[Array[Byte], Array[Byte]]] =
    Some(
      _.withBootstrapServers(
        container.bootstrapServers
      ).withProperties(
        Map(
          ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG             -> true.toString,
          ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> 1.toString,
          ProducerConfig.BATCH_SIZE_CONFIG                     -> 0.toString
        )
      ).withParallelism(1)
    )

  def createProducer(): ProducerSettings[Array[Byte], Array[Byte]] =
    ProducerSettings(system, new ByteArraySerializer, new ByteArraySerializer)
      .withBootstrapServers(container.bootstrapServers)

  /** Call this function to send a message after the next step of configured time period to trigger a rollover so the
    * current object will finish processing
    * @param duration
    * @param producerSettings
    * @param topic
    * @return
    */
  def sendTopicAfterTimePeriod(duration: FiniteDuration,
                               producerSettings: ProducerSettings[Array[Byte], Array[Byte]],
                               topic: String
  ): Future[Done] = akka.pattern.after(duration) {
    Source(
      List(
        new ProducerRecord[Array[Byte], Array[Byte]](topic, "1".getBytes, "1".getBytes)
      )
    ).runWith(Producer.plainSink(producerSettings))
  }

  protected var adminClient: AdminClient = _

  override def afterStart(): Unit = {
    super.afterStart()
    adminClient = AdminClient.create(
      Map[String, AnyRef](
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> container.bootstrapServers
      ).asJava
    )
  }

  override def beforeStop(): Unit = {
    adminClient.close()
    super.beforeStop()
  }

  def createTopics(topics: Set[String])(implicit executionContext: ExecutionContext): Future[Unit] =
    for {
      currentTopics <- adminClient.listTopics().names().toCompletableFuture.asScala
      topicsToCreate = topics.diff(currentTopics.asScala.toSet)
      _ <- adminClient
             .createTopics(topicsToCreate.map { topic =>
               new NewTopic(topic, 1, 1.toShort)
             }.asJava)
             .all()
             .toCompletableFuture
             .asScala
    } yield ()

  def cleanTopics(topics: Set[String])(implicit executionContext: ExecutionContext): Future[Unit] =
    for {
      currentTopics <- adminClient.listTopics().names().toCompletableFuture.asScala
      topicsToDelete = topics.intersect(currentTopics.asScala.toSet)
      _ <- adminClient.deleteTopics(topicsToDelete.asJava).all().toCompletableFuture.asScala
    } yield ()

  case object TerminationException extends Exception("termination-exception")
}
