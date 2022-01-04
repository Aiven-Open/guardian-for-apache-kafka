package io.aiven.guardian.akka

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.testkit.TestKitBase
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import scala.concurrent.duration._
import scala.language.postfixOps

trait AkkaStreamTestKit extends TestKitBase with BeforeAndAfterAll { this: Suite =>
  implicit val system: ActorSystem

  override protected def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  /** If its not possible to determine whether a Stream has finished in a test and instead you need to use a manual
    * wait, make sure you wait at least this period of time for akka-streams to initialize properly.
    */
  val AkkaStreamInitializationConstant: FiniteDuration = 500 millis
}
