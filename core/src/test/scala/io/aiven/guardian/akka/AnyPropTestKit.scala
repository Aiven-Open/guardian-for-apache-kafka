package io.aiven.guardian.akka

import akka.actor.ActorSystem
import akka.testkit.TestKitBase
import org.scalatest.propspec.AnyPropSpec

class AnyPropTestKit(_system: ActorSystem) extends AnyPropSpec with TestKitBase {
  implicit val system: ActorSystem = _system
}
