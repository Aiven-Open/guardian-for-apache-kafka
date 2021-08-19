package io.aiven.guardian.akka

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import org.scalatest.Suite

trait AkkaHttpTestKit extends AkkaStreamTestKit { this: Suite =>
  implicit val system: ActorSystem

  override protected def afterAll(): Unit =
    Http(system)
      .shutdownAllConnectionPools()
      .foreach { _ =>
        super.afterAll()
      }(system.dispatcher)
}
