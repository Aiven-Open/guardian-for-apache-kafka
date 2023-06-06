package io.aiven.guardian.pekko

import org.apache.pekko
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import scala.concurrent.duration._
import scala.language.postfixOps

import pekko.actor.ActorSystem
import pekko.testkit.TestKit
import pekko.testkit.TestKitBase

trait PekkoStreamTestKit extends TestKitBase with BeforeAndAfterAll { this: Suite =>
  implicit val system: ActorSystem

  override protected def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  /** If its not possible to determine whether a Stream has finished in a test and instead you need to use a manual
    * wait, make sure you wait at least this period of time for akka-streams to initialize properly.
    */
  val PekkoStreamInitializationConstant: FiniteDuration = 1 second
}
