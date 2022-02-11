package io.aiven.guardian.kafka.restore

import akka.Done
import akka.actor.ActorSystem
import io.aiven.guardian.kafka.restore.KafkaProducer
import io.aiven.guardian.kafka.restore.s3.RestoreClient

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait App {
  implicit val kafkaProducer: KafkaProducer
  implicit val restoreClient: RestoreClient[KafkaProducer]
  implicit val actorSystem: ActorSystem
  implicit val executionContext: ExecutionContext

  def run(): Future[Done] = restoreClient.restore
}
