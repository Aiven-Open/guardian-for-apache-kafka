package io.aiven.guardian.cli

import org.apache.pekko.actor.ActorSystem

import scala.concurrent.ExecutionContext

trait PekkoSettings {
  implicit val actorSystem: ActorSystem           = ActorSystem()
  implicit val executionContext: ExecutionContext = ExecutionContext.global
}
