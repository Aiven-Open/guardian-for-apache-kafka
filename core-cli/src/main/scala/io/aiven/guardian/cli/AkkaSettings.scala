package io.aiven.guardian.cli

import akka.actor.ActorSystem

import scala.concurrent.ExecutionContext

trait AkkaSettings {
  implicit val actorSystem: ActorSystem           = ActorSystem()
  implicit val executionContext: ExecutionContext = ExecutionContext.global
}
