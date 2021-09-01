package io.aiven.guardian.akka

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.testkit.TestKitBase
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

trait AkkaStreamTestKit extends TestKitBase with BeforeAndAfterAll { this: Suite =>
  implicit val system: ActorSystem

  override protected def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)
}
