package io.aiven.guardian.pekko

import org.apache.pekko
import org.scalatest.fixture
import org.scalatest.propspec.FixtureAnyPropSpecLike

import pekko.actor.ActorSystem
import pekko.testkit.TestKitBase

class AnyPropTestKit(_system: ActorSystem)
    extends FixtureAnyPropSpecLike
    with TestKitBase
    with fixture.TestDataFixture {
  implicit val system: ActorSystem = _system
}
