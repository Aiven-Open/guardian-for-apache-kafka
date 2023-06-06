package io.aiven.guardian.pekko

import org.apache.pekko
import org.scalatest.propspec.AnyPropSpec

import pekko.actor.ActorSystem
import pekko.testkit.TestKitBase

class AnyPropTestKit(_system: ActorSystem) extends AnyPropSpec with TestKitBase {
  implicit val system: ActorSystem = _system
}
